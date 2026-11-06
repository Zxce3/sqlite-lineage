# sqlite-lineage

Instrument SQLite queries to generate an interactive **data lineage graph** — trace any value in any
table back through every transformation to the raw file it came from. Zero dependencies (stdlib `sqlite3`).

```python
import sqlite_lineage as sl

db = sl.connect("pipeline.db")                    # drop-in sqlite3 connection
db.lineage.load_csv("raw/orders.csv", "raw_orders")   # registers the file as a source (path + sha256)
db.execute("CREATE TABLE stg AS SELECT id, CAST(amount AS REAL) amount FROM raw_orders")
db.execute("CREATE TABLE rev AS SELECT SUM(amount) AS revenue FROM stg")

db.lineage.upstream("rev", "revenue")             # {('stg','amount'), ('raw_orders','amount'), ...}
db.lineage.trace("stg", "amount", 10.0)           # value → tables → CSV line numbers of the raw file
db.lineage.export_html("lineage.html")            # interactive viewer (click a column = highlight ancestry)
```

CLI: `python -m sqlite_lineage pipeline.db -o lineage.html` (lineage is stored *inside* the DB in `_lineage_*` tables).

## How it works
`sqlite3.set_authorizer` reports the exact columns each statement reads and the tables it writes; a small SQL
scan of the SELECT list / `SET` clause maps outputs to inputs. When a statement is too complex to map
column-by-column (compound selects, etc.) every output column is linked to every input and the edge is flagged
`approx` (dashed in the viewer). Lineage rows live in the same transaction as the data.

## Limits (v0.1)
Column mapping is heuristic (name based, no alias scoping); `DELETE`/`DROP` don't prune edges; value tracing
compares exact values (as text), so aggregated values show structure only. Raw files are CSV via `load_csv`; use
`lineage.register_source(table, path)` for other formats.

## Dev
`pip install -e .` then `python -m unittest discover tests`, `python examples/demo.py`.
