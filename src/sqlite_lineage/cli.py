import argparse
import sqlite3

from .core import Lineage


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sqlite-lineage", description="Render lineage stored in a SQLite DB to HTML")
    ap.add_argument("db")
    ap.add_argument("-o", "--out", default="lineage.html")
    a = ap.parse_args(argv)
    conn = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    conn._ro_lineage = False
    lin = Lineage.__new__(Lineage)
    lin.conn, lin._busy, lin._reads, lin._writes = conn, False, set(), {}
    print(lin.export_html(a.out))


if __name__ == "__main__":
    main()
