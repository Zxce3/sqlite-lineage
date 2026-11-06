"""Core: an instrumented sqlite3 connection that records column-level lineage.

How it works
------------
* ``sqlite3.Connection.set_authorizer`` reports exactly which (table, column)
  pairs a statement READs and which tables it INSERTs/UPDATEs/CREATEs.
* A light SQL scan of the SELECT list / SET clause maps written columns to the
  read columns that feed them (falls back to "all reads" when it can't tell).
* Edges and file sources are persisted in ``_lineage_*`` tables inside the same
  database, so lineage travels with the data (and rolls back with it).
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path

_IDENT = r'"[^"]+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*'
_QUAL_RE = re.compile(rf"(?:({_IDENT})\s*\.\s*)?({_IDENT})")


def _unq(s: str) -> str:
    return s[1:-1] if s and s[0] in '"`[' else s


def _clean(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def _depths(s: str) -> list:
    d, out = 0, []
    for ch in s:
        if ch == ")":
            d -= 1
        out.append(d)
        if ch == "(":
            d += 1
    return out


def _find_top(s, word, start=0, depths=None):
    depths = depths or _depths(s)
    for m in re.finditer(rf"\b{word}\b", s[start:], re.I):
        i = start + m.start()
        if depths[i] == 0:
            return m.start() + start, m.end() + start
    return None


def _split_top(s: str) -> list:
    parts, d, cur = [], 0, []
    for ch in s:
        if ch == "(":
            d += 1
        elif ch == ")":
            d -= 1
        if ch == "," and d == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _select_items(sql: str):
    dep = _depths(sql)
    sel = _find_top(sql, "select", 0, dep)
    if not sel:
        return None
    frm = _find_top(sql, "from", sel[1], dep)
    body = sql[sel[1] : frm[0] if frm else len(sql)].rstrip("; \n")
    body = re.sub(r"^\s*(distinct|all)\b", "", body, flags=re.I)
    return _split_top(body)


def _set_items(sql: str):
    dep = _depths(sql)
    st = _find_top(sql, "set", 0, dep)
    if not st:
        return None
    ends = [m[0] for w in ("where", "from", "returning") if (m := _find_top(sql, w, st[1], dep))]
    body = sql[st[1] : min(ends) if ends else len(sql)].rstrip("; \n")
    out = {}
    for part in _split_top(body):
        if "=" in part:
            k, v = part.split("=", 1)
            out[_unq(k.strip().split(".")[-1]).lower()] = v
    return out


class Lineage:
    def __init__(self, conn: sqlite3.Connection, instrument: bool = True):
        self.conn = conn
        self._busy = False
        self._reads: set = set()
        self._writes: dict = {}
        self._ensure_schema()
        if instrument:
            conn.set_authorizer(self._auth)

    # ------------------------------------------------------------ storage
    def _q(self, sql, params=()):
        was, self._busy = self._busy, True
        try:
            return sqlite3.Cursor(self.conn).execute(sql, params).fetchall()
        finally:
            self._busy = was

    def _ensure_schema(self):
        if getattr(self.conn, "_ro_lineage", False):
            return
        was, self._busy = self._busy, True
        try:
            c = sqlite3.Cursor(self.conn)
            c.execute(
                "CREATE TABLE IF NOT EXISTS _lineage_edge("
                "src_table TEXT, src_col TEXT, dst_table TEXT, dst_col TEXT, sql TEXT, "
                "approx INTEGER, ts REAL, UNIQUE(src_table,src_col,dst_table,dst_col,sql))"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS _lineage_source("
                "tbl TEXT PRIMARY KEY, path TEXT, sha256 TEXT, rows INTEGER, loaded_at REAL)"
            )
        finally:
            self._busy = was

    # --------------------------------------------------------- authorizer
    def _auth(self, action, a1, a2, dbname, source):
        if self._busy or not a1 or str(a1).startswith(("sqlite_", "_lineage")):
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ and a2:
            self._reads.add((a1, a2))
        elif action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_TEMP_TABLE):
            self._writes.setdefault(a1, set())
        elif action == sqlite3.SQLITE_UPDATE:
            self._writes.setdefault(a1, set()).add(a2)
        return sqlite3.SQLITE_OK

    def _discard(self):
        self._reads, self._writes = set(), {}

    def _flush(self, sql: str):
        reads, writes = self._reads, self._writes
        self._discard()
        if not writes or not reads:
            return
        text = _clean(sql)
        by_name: dict = {}
        for t, c in reads:
            by_name.setdefault(c.lower(), []).append((t, c))

        def deps(expr):
            found = set()
            for q, n in _QUAL_RE.findall(expr):
                cands = by_name.get(_unq(n).lower(), [])
                if q:
                    narrowed = [x for x in cands if x[0].lower() == _unq(q).lower()]
                    cands = narrowed or cands
                found.update(cands)
            return found

        rows = []
        for table, upd in writes.items():
            approx = False
            mapping: dict = {}
            if upd:
                sets = _set_items(text) or {}
                for col in upd:
                    expr = sets.get(col.lower())
                    mapping[col] = deps(expr) if expr is not None else set(reads)
                    approx |= expr is None
            else:
                dcols = [r[1] for r in self._q(f'PRAGMA table_info("{table}")')]
                m = re.search(rf"insert\s+(?:or\s+\w+\s+)?into\s+(?:{_IDENT})(?:\s*\.\s*(?:{_IDENT}))?\s*\(([^)]*)\)", text, re.I)
                if m:
                    dcols = [_unq(x.strip()) for x in m.group(1).split(",")]
                items = _select_items(text)
                expanded = None
                if items is not None:
                    expanded = []
                    for it in items:
                        it = re.sub(r"\s+as\s+(?:{})\s*$".format(_IDENT), "", it, flags=re.I)
                        if it.endswith("*"):
                            qual = _unq(it[:-1].rstrip(". ").strip()) if it != "*" else None
                            tabs = [t for t in dict.fromkeys(t for t, _ in reads) if not qual or t.lower() == qual.lower()]
                            for t in tabs:
                                for r in self._q(f'PRAGMA table_info("{t}")'):
                                    expanded.append({(t, r[1])} & reads or {(t, r[1])})
                        else:
                            expanded.append(deps(it))
                if expanded is not None and len(expanded) == len(dcols):
                    mapping = dict(zip(dcols, expanded))
                else:
                    approx = True
                    mapping = {c: set(reads) for c in dcols}
            for dcol, srcs in mapping.items():
                for st, sc in srcs:
                    rows.append((st, sc, table, dcol, sql.strip(), int(approx), time.time()))
        if rows:
            was, self._busy = self._busy, True
            try:
                sqlite3.Cursor(self.conn).executemany(
                    "INSERT OR IGNORE INTO _lineage_edge VALUES(?,?,?,?,?,?,?)", rows
                )
            finally:
                self._busy = was

    # ------------------------------------------------------------ sources
    def register_source(self, table: str, path, rows: int | None = None):
        p = Path(path)
        digest = hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
        self._q(
            "INSERT OR REPLACE INTO _lineage_source VALUES(?,?,?,?,?)",
            (table, str(path), digest, rows, time.time()),
        )

    def load_csv(self, path, table: str, **csv_kwargs) -> int:
        """Load a CSV into ``table`` (all TEXT columns, rowid == line number - 1)
        and register the file as the lineage source of that table."""
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.reader(f, **csv_kwargs)
            header = next(r)
            data = list(r)
        cols = ", ".join(f'"{h}" TEXT' for h in header)
        cur = sqlite3.Cursor(self.conn)
        was, self._busy = self._busy, True
        try:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'CREATE TABLE "{table}" ({cols})')
            cur.executemany(f'INSERT INTO "{table}" VALUES ({",".join("?" * len(header))})', data)
        finally:
            self._busy = was
        self.register_source(table, path, len(data))
        return len(data)

    # -------------------------------------------------------------- query
    def edges(self):
        return self._q("SELECT src_table,src_col,dst_table,dst_col,sql,approx FROM _lineage_edge")

    def sources(self) -> dict:
        return {t: {"path": p, "sha256": h, "rows": n} for t, p, h, n, _ in self._q("SELECT * FROM _lineage_source")}

    def upstream(self, table: str, column: str | None = None) -> set:
        """All (table, column) nodes that feed ``table[.column]``, transitively."""
        edges = self.edges()
        todo = [(table, column)] if column else list({(e[2], e[3]) for e in edges if e[2] == table})
        seen = set()
        while todo:
            t, c = todo.pop()
            for st, sc, dt, dc, *_ in edges:
                if dt == t and (c is None or dc == c) and (st, sc) not in seen:
                    seen.add((st, sc))
                    todo.append((st, sc))
        return seen

    def trace(self, table: str, column: str, value, max_depth: int = 25) -> dict:
        """Trace ``value`` of ``table.column`` upstream. Each node reports how
        many rows hold that exact value (``matches``); where a transformation
        changed it (e.g. SUM), ``matched`` is False and the structure alone is
        followed. Source tables list the raw file and CSV line numbers."""
        edges = self.edges()
        srcs = self.sources()

        def count(t, c, v):
            if v is None:
                return 0, []
            q = f'FROM "{t}" WHERE CAST("{c}" AS TEXT) = CAST(? AS TEXT)'
            n = self._q(f"SELECT COUNT(*) {q}", (v,))[0][0]
            ids = [r[0] for r in self._q(f"SELECT rowid {q} LIMIT 5", (v,))]
            return n, ids

        def walk(t, c, v, depth, path):
            n, ids = count(t, c, v)
            node = {"node": f"{t}.{c}", "matches": n, "matched": n > 0, "children": []}
            if t in srcs:
                node["source"] = {**srcs[t], "csv_lines": [i + 1 for i in ids]}
            if depth < max_depth:
                for st, sc, dt, dc, sql, approx in edges:
                    if (dt, dc) == (t, c) and (st, sc) not in path:
                        child = walk(st, sc, v if n else None, depth + 1, path | {(st, sc)})
                        child["via_sql"], child["approx"] = sql, bool(approx)
                        node["children"].append(child)
            return node

        return walk(table, column, value, 0, {(table, column)})

    # -------------------------------------------------------------- graph
    def graph(self) -> dict:
        nodes: dict = {}
        links = []
        for st, sc, dt, dc, sql, approx in self.edges():
            for t, c in ((st, sc), (dt, dc)):
                nodes.setdefault(f"{t}.{c}", {"id": f"{t}.{c}", "table": t, "column": c, "kind": "column"})
            links.append({"source": f"{st}.{sc}", "target": f"{dt}.{dc}", "sql": sql, "approx": bool(approx)})
        for t, info in self.sources().items():
            fid = f"file:{info['path']}"
            nodes[fid] = {"id": fid, "table": t, "column": Path(info["path"]).name, "kind": "file", **info}
            cols = [r[1] for r in self._q(f'PRAGMA table_info("{t}")')]
            for c in cols:
                nodes.setdefault(f"{t}.{c}", {"id": f"{t}.{c}", "table": t, "column": c, "kind": "column"})
                links.append({"source": fid, "target": f"{t}.{c}", "sql": f"loaded from {info['path']}", "approx": False})
        return {"nodes": list(nodes.values()), "links": links}

    def export_html(self, path="lineage.html") -> str:
        tpl = (Path(__file__).parent / "viewer.html").read_text(encoding="utf-8")
        data = json.dumps(self.graph()).replace("</", "<\\/")
        Path(path).write_text(tpl.replace("__GRAPH_JSON__", data), encoding="utf-8")
        return str(path)


class _Cursor(sqlite3.Cursor):
    def _run(self, fn, sql, params):
        lin = self.connection._lin
        try:
            r = fn(sql, params)
        except BaseException:
            if lin:
                lin._discard()
            raise
        if lin:
            lin._flush(sql)
        return r

    def execute(self, sql, params=()):
        return self._run(super().execute, sql, params)

    def executemany(self, sql, params):
        return self._run(super().executemany, sql, params)


class Connection(sqlite3.Connection):
    _lin: Lineage | None = None

    @property
    def lineage(self) -> Lineage:
        return self._lin

    def cursor(self, factory=_Cursor):
        return super().cursor(factory)

    def execute(self, sql, params=()):
        return self.cursor().execute(sql, params)

    def executemany(self, sql, params):
        return self.cursor().executemany(sql, params)

    def executescript(self, script):
        buf = ""
        for piece in re.split(r"(;)", script):
            buf += piece
            if piece == ";" and sqlite3.complete_statement(buf):
                self.cursor().execute(buf)
                buf = ""
        if buf.strip():
            self.cursor().execute(buf)
        return self.cursor()


def connect(database=":memory:", **kwargs) -> Connection:
    """Open a SQLite database with lineage tracking enabled (``conn.lineage``)."""
    conn = sqlite3.connect(database, factory=Connection, **kwargs)
    conn._lin = Lineage(conn)
    return conn
