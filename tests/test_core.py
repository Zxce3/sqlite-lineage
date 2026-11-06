import csv
import tempfile
import unittest
from pathlib import Path

import sqlite_lineage as sl


class LineageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        with open(self.tmp / "orders.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerows([["id", "customer", "amount"], [1, "ann", 10], [2, "bob", 25], [3, "ann", 5]])
        self.db = sl.connect(":memory:")
        self.db.lineage.load_csv(self.tmp / "orders.csv", "raw_orders")

    def test_ctas_column_mapping(self):
        self.db.execute(
            "CREATE TABLE totals AS SELECT customer AS who, SUM(CAST(amount AS INT)) AS total "
            "FROM raw_orders GROUP BY customer"
        )
        up = self.db.lineage.upstream("totals", "total")
        self.assertEqual(up, {("raw_orders", "amount")})
        self.assertEqual(self.db.lineage.upstream("totals", "who"), {("raw_orders", "customer")})

    def test_multi_hop_and_trace_to_file(self):
        self.db.execute("CREATE TABLE t1 AS SELECT customer, amount FROM raw_orders")
        self.db.execute("CREATE TABLE t2(name TEXT, amt TEXT)")
        self.db.execute("INSERT INTO t2(name, amt) SELECT customer, amount FROM t1 WHERE customer='bob'")
        tree = self.db.lineage.trace("t2", "amt", "25")
        self.assertTrue(tree["matched"])
        leaf = tree["children"][0]["children"][0]
        self.assertEqual(leaf["node"], "raw_orders.amount")
        self.assertEqual(leaf["source"]["csv_lines"], [3])  # header is line 1
        self.assertTrue(leaf["source"]["path"].endswith("orders.csv"))

    def test_update_and_persistence(self):
        self.db.execute("CREATE TABLE t(a, b)")
        self.db.execute("INSERT INTO t SELECT id, amount FROM raw_orders")
        self.db.execute("UPDATE t SET b = a * 2")
        self.assertIn(("t", "a"), self.db.lineage.upstream("t", "b"))
        # lineage is stored in the DB itself
        n = self.db.execute("SELECT COUNT(*) FROM _lineage_edge").fetchone()[0]
        self.assertGreater(n, 0)

    def test_values_insert_records_nothing_and_html(self):
        self.db.execute("CREATE TABLE x(a)")
        self.db.execute("INSERT INTO x VALUES (1)")
        self.assertEqual(self.db.lineage.upstream("x", "a"), set())
        out = self.db.lineage.export_html(self.tmp / "l.html")
        self.assertIn("raw_orders", Path(out).read_text())

    def test_star_and_executescript(self):
        self.db.executescript("CREATE TABLE c AS SELECT * FROM raw_orders; CREATE TABLE d AS SELECT id FROM c;")
        self.assertEqual(self.db.lineage.upstream("d", "id"), {("c", "id"), ("raw_orders", "id")})


if __name__ == "__main__":
    unittest.main()
