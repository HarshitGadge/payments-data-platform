"""Silver: decode Bronze change events and MERGE them into the payments table.

Runs as a ``foreachBatch`` stream. The MERGE is a batch operation -- Structured
Streaming cannot express it in a plain sink -- so each micro-batch is collapsed
to one row per key and applied with the LSN guard in
:mod:`payments_platform.merge`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from payments_platform.config import IcebergConfig  # noqa: E402
from payments_platform.merge import create_silver_table, merge_payments  # noqa: E402
from payments_platform.session import configure_session  # noqa: E402
from payments_platform.transforms import (  # noqa: E402
    decoded_payments,
    latest_change_per_key,
    quarantine,
)

from bronze_cdc import BRONZE_TABLE  # noqa: E402

SILVER_TABLE = "payments"
QUARANTINE_TABLE = "payments_quarantine"


def process_batch(batch_df, batch_id: int, spark, iceberg: IcebergConfig) -> None:
    """Decode, quarantine, collapse and merge one micro-batch."""
    decoded = decoded_payments(batch_df).cache()
    try:
        bad = quarantine(decoded)
        if not bad.isEmpty():
            bad.writeTo(iceberg.table("silver", QUARANTINE_TABLE)).append()

        collapsed = latest_change_per_key(decoded)
        if not collapsed.isEmpty():
            merge_payments(spark, collapsed, iceberg.table("silver", SILVER_TABLE))
        print(f"batch {batch_id}: merged {collapsed.count()} rows")
    finally:
        decoded.unpersist()


def main() -> None:
    iceberg = IcebergConfig()
    spark = configure_session(SparkSession.builder.getOrCreate(), iceberg)
    create_silver_table(spark, iceberg.table("silver", SILVER_TABLE))

    query = (
        spark.readStream.table(iceberg.table("bronze", BRONZE_TABLE))
        .writeStream.foreachBatch(
            lambda df, batch_id: process_batch(df, batch_id, spark, iceberg)
        )
        .option("checkpointLocation", f"{iceberg.warehouse}/_checkpoints/{SILVER_TABLE}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print("Silver merge complete")


if __name__ == "__main__":
    main()
