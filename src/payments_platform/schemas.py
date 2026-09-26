"""Explicit Spark schemas for the lakehouse layers.

Declared rather than inferred: a streaming job that infers its schema changes
table shape the first time a source column is added, and the write then fails
mid-stream against the existing Iceberg table.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: What the CDC decode UDF returns for one Debezium envelope.
#:
#: ``amount`` is carried as a *string* and cast to DECIMAL in SQL immediately
#: after. Arrow's decimal128 round-trip through pandas is fragile across
#: pyarrow versions, and a string -> DECIMAL cast in Spark is exact, so this
#: gives precise money without depending on that round-trip.
DECODED_PAYMENT = StructType(
    [
        StructField("payment_id", LongType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("shopper_id", LongType(), True),
        StructField("amount_str", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("cdc_op", StringType(), True),
        StructField("cdc_lsn", LongType(), True),
        StructField("cdc_source_ts", TimestampType(), True),
        StructField("is_deleted", BooleanType(), True),
        StructField("is_tombstone", BooleanType(), True),
        StructField("decode_ok", BooleanType(), True),
    ]
)

DECODED_PAYMENT_FIELDS = tuple(f.name for f in DECODED_PAYMENT.fields)

#: Silver table DDL. Written as SQL rather than a StructType because Iceberg
#: table creation carries partitioning and table properties alongside the
#: columns.
SILVER_PAYMENTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    payment_id      BIGINT       COMMENT 'Primary key from the source system',
    merchant_id     BIGINT,
    shopper_id      BIGINT,
    amount          DECIMAL(12,2) COMMENT 'Original currency, never a float',
    currency        STRING,
    payment_method  STRING,
    payment_status  STRING,
    country_code    STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    cdc_lsn         BIGINT        COMMENT 'Postgres WAL position; ordering authority',
    cdc_source_ts   TIMESTAMP,
    ingested_at     TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'write.delete.mode'                 = 'merge-on-read',
    'write.update.mode'                 = 'merge-on-read',
    'write.merge.mode'                  = 'merge-on-read',
    'format-version'                    = '2'
)
"""
