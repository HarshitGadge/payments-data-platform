"""CDC merge semantics against a real Iceberg table.

These assert the four properties in payments_platform.merge: deletes remove
rows, replays do not move the table backwards, a multi-change batch collapses
to one row per key, and a delete for an unknown row inserts nothing.
"""

from __future__ import annotations

import base64
import json
from decimal import Decimal

import pytest

# These import pyspark transitively; skip the whole module without it
# rather than erroring during collection on a clone with no dev deps.
pytest.importorskip("pyspark")

from payments_platform.merge import create_silver_table, merge_payments  # noqa: E402
from payments_platform.transforms import (  # noqa: E402
    decoded_payments,
    latest_change_per_key,
)

TABLE = "lakehouse.silver.payments"


def unscaled_b64(unscaled: int) -> str:
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(length, "big", signed=True)).decode()


def envelope(payment_id, *, op="c", amount=14999, status="authorized", lsn=100):
    row = {
        "payment_id": payment_id,
        "merchant_id": 1,
        "shopper_id": 501,
        "amount": unscaled_b64(amount),
        "currency": "EUR",
        "payment_method": "card",
        "payment_status": status,
        "country_code": "NL",
        "created_at": 1767225600000000,
        "updated_at": 1767225600000000,
    }
    return json.dumps(
        {
            "before": row if op == "d" else None,
            "after": None if op == "d" else row,
            "source": {"table": "payments", "lsn": lsn, "ts_ms": 1767225600000},
            "op": op,
        }
    )


@pytest.fixture
def silver(spark):
    """A fresh silver table per test."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    create_silver_table(spark, TABLE)
    return TABLE


def apply_batch(spark, table, messages):
    """Decode, collapse and merge one batch of Kafka message values."""
    bronze = spark.createDataFrame([(m,) for m in messages], "value string")
    batch = latest_change_per_key(decoded_payments(bronze))
    merge_payments(spark, batch, table)


def rows(spark, table):
    return {r.payment_id: r for r in spark.table(table).collect()}


class TestDecodeIntoIceberg:
    def test_insert_lands_with_exact_decimal_amount(self, spark, silver):
        apply_batch(spark, silver, [envelope(1001, amount=14999)])
        row = rows(spark, silver)[1001]
        # Not 149.99000000000001: the column is DECIMAL, not DOUBLE.
        assert row.amount == Decimal("149.99")

    def test_timestamps_decode_as_microseconds(self, spark, silver):
        # Asserted through Spark, not collect(): collect() renders a timestamp
        # as a naive datetime in the *driver's* local zone, so this assertion
        # would pass or fail depending on where the suite is run.
        apply_batch(spark, silver, [envelope(1001)])
        rendered = spark.sql(
            f"SELECT date_format(created_at, 'yyyy-MM-dd HH:mm:ss') AS ts FROM {silver}"
        ).collect()[0].ts
        # 1767225600000000 microseconds = 2026-01-01T00:00:00Z. Read as millis
        # it lands 50,000 years out; divided by 1000 out of habit, in 1970.
        assert rendered == "2026-01-01 00:00:00"

    def test_sum_of_many_rows_is_exact(self, spark, silver):
        # 300 payments of 0.10 must total exactly 30.00.
        apply_batch(spark, silver, [envelope(2000 + i, amount=10) for i in range(300)])
        total = spark.sql(f"SELECT SUM(amount) AS t FROM {silver}").collect()[0].t
        assert total == Decimal("30.00")


class TestMergeSemantics:
    def test_update_applies_the_newer_image(self, spark, silver):
        apply_batch(spark, silver, [envelope(1001, op="c", status="authorized", lsn=100)])
        apply_batch(spark, silver, [envelope(1001, op="u", status="refunded", lsn=200)])
        assert rows(spark, silver)[1001].payment_status == "refunded"

    def test_delete_removes_the_row(self, spark, silver):
        apply_batch(spark, silver, [envelope(1001, op="c", lsn=100)])
        apply_batch(spark, silver, [envelope(1001, op="d", lsn=200)])
        assert 1001 not in rows(spark, silver)

    def test_replayed_older_event_does_not_move_the_table_backwards(self, spark, silver):
        # The LSN guard is what makes a Kafka offset replay idempotent.
        apply_batch(spark, silver, [envelope(1001, op="c", status="authorized", lsn=100)])
        apply_batch(spark, silver, [envelope(1001, op="u", status="refunded", lsn=300)])
        apply_batch(spark, silver, [envelope(1001, op="u", status="authorized", lsn=100)])
        assert rows(spark, silver)[1001].payment_status == "refunded"

    def test_replaying_an_identical_batch_is_a_no_op(self, spark, silver):
        batch = [envelope(1001, lsn=100), envelope(1002, lsn=101)]
        apply_batch(spark, silver, batch)
        apply_batch(spark, silver, batch)
        assert spark.table(silver).count() == 2

    def test_multiple_changes_to_one_row_in_one_batch(self, spark, silver):
        # Iceberg rejects a MERGE whose source matches a target row twice, so
        # the batch must be collapsed first. Out-of-order on purpose.
        apply_batch(
            spark,
            silver,
            [
                envelope(1001, op="c", status="authorized", lsn=100),
                envelope(1001, op="u", status="refunded", lsn=300),
                envelope(1001, op="u", status="captured", lsn=200),
            ],
        )
        table_rows = rows(spark, silver)
        assert len(table_rows) == 1
        assert table_rows[1001].payment_status == "refunded"

    def test_insert_then_delete_within_one_batch_leaves_nothing(self, spark, silver):
        apply_batch(
            spark,
            silver,
            [envelope(1001, op="c", lsn=100), envelope(1001, op="d", lsn=200)],
        )
        assert rows(spark, silver) == {}

    def test_delete_for_an_unknown_row_inserts_nothing(self, spark, silver):
        apply_batch(spark, silver, [envelope(9999, op="d", lsn=500)])
        assert spark.table(silver).count() == 0

    def test_snapshot_read_backfills_the_table(self, spark, silver):
        # op='r' on first connector start must upsert, not be ignored.
        apply_batch(spark, silver, [envelope(1001, op="r", lsn=1), envelope(1002, op="r", lsn=2)])
        assert spark.table(silver).count() == 2


class TestQuarantine:
    def test_malformed_and_tombstone_messages_never_reach_the_merge(self, spark, silver):
        apply_batch(spark, silver, [envelope(1001), "not json at all", None])
        assert list(rows(spark, silver)) == [1001]
