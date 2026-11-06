"""Run: python examples/demo.py  ->  writes lineage.html"""
import csv, tempfile, json
from pathlib import Path
import sqlite_lineage as sl

tmp = Path(tempfile.mkdtemp())
with open(tmp / "orders.csv", "w", newline="") as f:
    csv.writer(f).writerows([["id", "customer", "amount"], [1, "ann", 10], [2, "bob", 25], [3, "ann", 5]])
with open(tmp / "customers.csv", "w", newline="") as f:
    csv.writer(f).writerows([["name", "country"], ["ann", "ID"], ["bob", "US"]])

db = sl.connect("demo.db")
db.lineage.load_csv(tmp / "orders.csv", "raw_orders")
db.lineage.load_csv(tmp / "customers.csv", "raw_customers")
db.execute("DROP TABLE IF EXISTS stg_orders"); db.execute("DROP TABLE IF EXISTS revenue_by_country")
db.execute("CREATE TABLE stg_orders AS SELECT o.id, o.customer, CAST(o.amount AS REAL) AS amount, c.country "
           "FROM raw_orders o JOIN raw_customers c ON c.name = o.customer")
db.execute("CREATE TABLE revenue_by_country AS SELECT country, SUM(amount) AS revenue FROM stg_orders GROUP BY country")
db.commit()

print(json.dumps(db.lineage.trace("revenue_by_country", "revenue", 15.0), indent=2))
print("wrote", db.lineage.export_html("lineage.html"))
