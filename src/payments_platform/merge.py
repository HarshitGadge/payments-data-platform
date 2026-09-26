"""Applying a CDC batch to the silver Iceberg table.

The MERGE is where CDC correctness is actually won or lost. Four properties
matter, and each maps to one clause below:

1. **A delete must delete.** Debezium sends the row in ``before``; if the job
   only ever upserts the ``after`` image, deleted rows live on in the warehouse
   forever and every downstream count is wrong.
2. **A replay must not move the table backwards.** Kafka offsets get replayed
   after a failed batch or a checkpoint reset. Without a guard, an older event
   re-applies over a newer one and a refunded payment reverts to authorized.
   The ``cdc_lsn > target.cdc_lsn`` predicate makes the MERGE idempotent.
3. **The source must hold one row per key.** Iceberg fails a MERGE whose source
   matches a target row more than once -- deliberately, since the result would
   be non-deterministic. The batch is collapsed by LSN before it gets here.
4. **A delete for an unseen row is a no-op**, not an insert of a tombstone.
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession

from .schemas import SILVER_PAYMENTS_DDL

#: Columns written to silver, in table order.
SILVER_COLUMNS: Sequence[str] = (
    "payment_id",
    "merchant_id",
    "shopper_id",
    "amount",
    "currency",
    "payment_method",
    "payment_status",
    "country_code",
    "created_at",
    "updated_at",
    "cdc_lsn",
    "cdc_source_ts",
    "ingested_at",
)


def create_silver_table(spark: SparkSession, table: str) -> None:
    """Create the silver payments table if it does not exist."""
    spark.sql(SILVER_PAYMENTS_DDL.format(table=table))


def merge_payments(
    spark: SparkSession,
    batch: DataFrame,
    table: str,
    key: str = "payment_id",
    source_view: str = "cdc_payment_batch",
) -> None:
    """Apply one collapsed CDC batch to the silver table.

    ``batch`` must already hold at most one row per ``key``; see
    :func:`payments_platform.transforms.latest_change_per_key`.
    """
    batch.createOrReplaceTempView(source_view)

    assignments = ",\n            ".join(
        f"t.{column} = s.{column}" for column in SILVER_COLUMNS
    )
    insert_columns = ", ".join(SILVER_COLUMNS)
    insert_values = ", ".join(f"s.{column}" for column in SILVER_COLUMNS)

    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING {source_view} AS s
          ON t.{key} = s.{key}

        -- (1) a delete must actually remove the row
        WHEN MATCHED AND s.is_deleted THEN DELETE

        -- (2) only move forward: an older replayed event is ignored
        WHEN MATCHED AND s.cdc_lsn > t.cdc_lsn THEN UPDATE SET
            {assignments}

        -- (4) a delete for a row we never saw inserts nothing
        WHEN NOT MATCHED AND NOT s.is_deleted THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """
    )
