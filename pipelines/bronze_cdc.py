"""Bronze: land Debezium change events from Kafka into Iceberg, undecoded.

Bronze stores the message exactly as Kafka delivered it, with the offset
coordinates beside it. No parsing happens here on purpose: a CDC topic has
finite retention, so once an event ages off the broker, Bronze is the only
copy. Keeping it raw means a decoder fix is replayed from Bronze rather than
re-snapshotted from Postgres, which on a large table means hours of load on the
production database.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from payments_platform.config import IcebergConfig, KafkaConfig  # noqa: E402
from payments_platform.session import configure_session  # noqa: E402

BRONZE_TABLE = "payments_cdc_raw"


def to_bronze(records):
    """Project a Kafka record into the Bronze table shape."""
    return records.select(
        F.col("value").cast("string").alias("value"),
        F.col("key").cast("string").alias("message_key"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def main() -> None:
    iceberg = IcebergConfig()
    spark = configure_session(SparkSession.builder.getOrCreate(), iceberg)
    kafka = KafkaConfig()
    target = iceberg.table("bronze", BRONZE_TABLE)

    stream = (
        spark.readStream.format("kafka")
        .options(**kafka.source_options(kafka.payments_topic))
        .load()
    )

    query = (
        to_bronze(stream)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{iceberg.warehouse}/_checkpoints/{BRONZE_TABLE}")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    print(f"Bronze ingest complete -> {target}")


if __name__ == "__main__":
    main()
