#!/usr/bin/env python3
"""End-to-end walkthrough with no infrastructure.

    python demo.py

Runs the real code paths -- Debezium decoding, the Iceberg CDC merge, the gold
aggregation, the FastAPI serving tier and FX conversion -- against sample data.
No Postgres, Kafka, Trino, Snowflake or cloud account required.

Sections needing an optional dependency (pyspark plus the Iceberg jar, duckdb)
announce themselves and skip rather than failing.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

RULE = "=" * 74


def header(text: str) -> None:
    print(f"\n{RULE}\n  {text}\n{RULE}")


def unscaled_b64(unscaled: int) -> str:
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(length, "big", signed=True)).decode()


def envelope(payment_id, *, op="c", amount=14999, status="authorized", lsn=100):
    row = {
        "payment_id": payment_id, "merchant_id": 1, "shopper_id": 501,
        "amount": unscaled_b64(amount), "currency": "EUR",
        "payment_method": "card", "payment_status": status, "country_code": "NL",
        "created_at": 1767225600000000, "updated_at": 1767225600000000,
    }
    return json.dumps({
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "source": {"table": "payments", "lsn": lsn, "ts_ms": 1767225600000},
        "op": op,
    })


def show_cdc_decoding() -> None:
    header("CDC  what Debezium sends, and what it decodes to")
    from payments_platform.cdc import decode_decimal, decode_micro_timestamp, parse_envelope

    raw = json.loads(envelope(1001))
    print("  raw 'after' image on the wire:")
    for key in ("payment_id", "amount", "currency", "created_at"):
        print(f"    {key:14s} {raw['after'][key]!r}")

    print("\n  decoded:")
    print(f"    amount         {decode_decimal(raw['after']['amount'], 2)}"
          "   <- base64 unscaled int, kept as Decimal")
    print(f"    created_at     {decode_micro_timestamp(raw['after']['created_at'])}"
          "   <- microseconds, not millis")
    print(f"    op             {parse_envelope(raw).op!r} (insert)")
    print("\n  [ok] read as a plain string, amount would be 'Opc='")
    print("  [ok] divided by 1000 out of habit, created_at would land in 1970")


def show_merge_ordering() -> None:
    header("CDC  collapsing an out-of-order batch by LSN")
    from payments_platform.cdc import latest_per_key, parse_envelope

    batch = [
        parse_envelope(json.loads(envelope(1001, op="c", status="authorized", lsn=100))),
        parse_envelope(json.loads(envelope(1001, op="u", status="refunded", lsn=300))),
        parse_envelope(json.loads(envelope(1001, op="u", status="captured", lsn=200))),
    ]
    print("  batch as it arrived from the partition:")
    for event in batch:
        print(f"    lsn={event.lsn:<5} op={event.op}  status={event.row['payment_status']}")
    winner = latest_per_key(batch, ["payment_id"])[0]
    print(f"\n  collapsed to one row per key -> status={winner.row['payment_status']!r} (lsn 300)")
    print("  [ok] Iceberg rejects a MERGE matching a target row twice; this is why")


def show_fx() -> None:
    header("FX  converting at the rate effective on the payment date")
    from payments_platform.fx import RateTable, parse_rates_response

    friday, saturday, monday = dt.date(2026, 1, 2), dt.date(2026, 1, 3), dt.date(2026, 1, 5)
    rates = parse_rates_response({"base": "USD", "date": friday.isoformat(), "rates": {"EUR": "0.92"}})
    rates += parse_rates_response({"base": "USD", "date": monday.isoformat(), "rates": {"EUR": "0.95"}})
    table = RateTable(rates)

    print(f"  published: Fri {friday} EUR 0.92    Mon {monday} EUR 0.95")
    print("             (nothing for Sat/Sun -- the FX market is closed)\n")
    for day, label in ((friday, "Friday"), (saturday, "Saturday"), (monday, "Monday")):
        carried = "   <- carried forward from Friday" if day == saturday else ""
        print(f"    EUR 100.00 on {label:9s} -> USD {table.to_base(Decimal('100.00'), 'EUR', day)}{carried}")
    print(f"\n    GBP 100.00 with no rate  -> {table.to_base(Decimal('100.00'), 'GBP', friday)}")
    print("  [ok] not defaulted to 1.0, which would report GBP as USD")


def show_iceberg_merge() -> None:
    header("LAKEHOUSE  CDC merge into a real Iceberg table")
    jar = os.environ.get("ICEBERG_JAR", "")
    if not jar or not Path(jar).exists():
        print("  ICEBERG_JAR not set - skipping.")
        print("  See README 'Running the tests' for how to fetch the Iceberg runtime jar.")
        return
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("  pyspark not installed - skipping. pip install -r requirements-dev.txt")
        return

    import tempfile

    os.environ.setdefault("PYTHONPATH", os.pathsep.join([str(ROOT / "src"), str(ROOT)]))
    spark = (
        SparkSession.builder.master("local[2]").appName("payments-demo")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", tempfile.mkdtemp(prefix="demo_iceberg_"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    from payments_platform.merge import create_silver_table, merge_payments
    from payments_platform.transforms import decoded_payments, latest_change_per_key

    table = "lakehouse.silver.payments"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    create_silver_table(spark, table)

    def apply(messages, label):
        bronze = spark.createDataFrame([(m,) for m in messages], "value string")
        merge_payments(spark, latest_change_per_key(decoded_payments(bronze)), table)
        print(f"    {label:<48s} rows={spark.table(table).count()}")

    apply([envelope(1001, lsn=100), envelope(1002, lsn=101)], "insert 1001, 1002")
    apply([envelope(1001, op="u", status="refunded", lsn=300)], "update 1001 -> refunded")
    apply([envelope(1001, op="u", status="authorized", lsn=100)], "REPLAY stale lsn=100 (must be ignored)")
    status = spark.sql(f"SELECT payment_status FROM {table} WHERE payment_id=1001").collect()[0][0]
    print(f"    {'1001 status after stale replay':<48s} {status!r}")
    apply([envelope(1002, op="d", lsn=400)], "delete 1002")

    print("\n  final table:")
    spark.sql(
        f"SELECT payment_id, amount, currency, payment_status FROM {table} ORDER BY payment_id"
    ).show(truncate=False)
    spark.stop()


def show_api() -> None:
    header("API  serving tier over gold")
    try:
        import duckdb
        from fastapi.testclient import TestClient
    except ImportError:
        print("  fastapi/duckdb not installed - skipping.")
        return

    from api.main import create_app
    from api.repository import SqlRepository

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA gold")
    con.execute("""CREATE TABLE gold.daily_merchant_revenue (
        revenue_date DATE, merchant_id BIGINT, currency VARCHAR, payment_count BIGINT,
        gross_amount DECIMAL(12,2), average_amount DECIMAL(12,2), distinct_shoppers BIGINT)""")
    con.execute("""INSERT INTO gold.daily_merchant_revenue VALUES
        ('2026-01-01', 1, 'EUR', 3, 1234567.89, 411522.63, 3),
        ('2026-01-01', 1, 'USD', 2, 500.00, 250.00, 2)""")
    con.execute("""CREATE TABLE gold.payment_method_mix (
        activity_date DATE, country_code VARCHAR, payment_method VARCHAR,
        payment_count BIGINT, share_pct DOUBLE)""")

    client = TestClient(create_app(SqlRepository(con)))
    params = {"start_date": "2026-01-01", "end_date": "2026-01-01"}
    print("  GET /v1/merchants/1/revenue?start_date=2026-01-01&end_date=2026-01-01\n")
    body = client.get("/v1/merchants/1/revenue", params=params).json()
    print(json.dumps({k: body[k] for k in ("merchant_id", "currencies", "total_by_currency")}, indent=4))
    print("\n  [ok] amounts are JSON strings: 1234567.89 sent as a JSON number")
    print("       comes back as 1234567.8899999999 in most clients")
    print(f"  [ok] unknown merchant     -> {client.get('/v1/merchants/999/revenue', params=params).status_code}")
    print(f"  [ok] reversed date range  -> "
          f"{client.get('/v1/merchants/1/revenue', params={'start_date':'2026-01-02','end_date':'2026-01-01'}).status_code}")


def main() -> None:
    print("\n  Payments Data Platform - offline demo")
    print("  Streaming CDC lakehouse + FastAPI serving + batch FX warehouse")
    show_cdc_decoding()
    show_merge_ordering()
    show_fx()
    show_iceberg_merge()
    show_api()
    print(f"\n{RULE}\n  Done. `pytest` runs the full suite; see docs/ for the design.\n{RULE}\n")


if __name__ == "__main__":
    main()
