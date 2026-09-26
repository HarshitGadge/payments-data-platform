"""Spark transforms binding the pure CDC decoder to DataFrames."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from .cdc import (
    OP_DELETE,
    decode_decimal,
    decode_micro_timestamp,
    parse_envelope,
)
from .schemas import DECODED_PAYMENT, DECODED_PAYMENT_FIELDS

#: Scale of the payments.amount NUMERIC(12,2) column in the source schema.
AMOUNT_SCALE = 2

_EMPTY = {name: None for name in DECODED_PAYMENT_FIELDS}


def decode_payment(raw: Any) -> Dict[str, Any]:
    """Decode one Kafka message value into the flat payment row.

    Always returns every key so the struct shape is stable; ``decode_ok`` is
    False for anything unparseable, which routes the record to quarantine
    rather than failing the batch.
    """
    record = dict(_EMPTY)
    record["decode_ok"] = False
    record["is_tombstone"] = False
    record["is_deleted"] = False

    if raw is None:
        record["is_tombstone"] = True
        return record
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return record

    event = parse_envelope(raw)
    if event is None:
        return record
    if event.is_tombstone:
        record["is_tombstone"] = True
        return record

    row = event.row or {}
    amount = decode_decimal(row.get("amount"), AMOUNT_SCALE)
    record.update(
        payment_id=_as_int(row.get("payment_id")),
        merchant_id=_as_int(row.get("merchant_id")),
        shopper_id=_as_int(row.get("shopper_id")),
        amount_str=str(amount) if amount is not None else None,
        currency=row.get("currency"),
        payment_method=row.get("payment_method"),
        payment_status=row.get("payment_status"),
        country_code=row.get("country_code"),
        created_at=decode_micro_timestamp(row.get("created_at")),
        updated_at=decode_micro_timestamp(row.get("updated_at")),
        cdc_op=event.op,
        cdc_lsn=event.lsn,
        cdc_source_ts=event.source_ts,
        is_deleted=event.op == OP_DELETE,
        is_tombstone=False,
        decode_ok=_as_int(row.get("payment_id")) is not None,
    )
    return record


def _as_int(value: Any):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@pandas_udf(DECODED_PAYMENT)
def decode_payment_udf(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    """Arrow-vectorised decode of a column of Kafka message values.

    Iterator form so the JVM streams batches through one worker process rather
    than materialising the whole partition.
    """
    for values in batches:
        rows = [decode_payment(v) for v in values]
        yield pd.DataFrame(rows, columns=list(DECODED_PAYMENT_FIELDS))


def decoded_payments(bronze: DataFrame, value_column: str = "value") -> DataFrame:
    """Bronze Kafka records -> typed payment columns, ready to MERGE.

    ``amount`` is cast from its string carrier to DECIMAL(12,2) here, so every
    downstream sum is exact.
    """
    decoded = bronze.withColumn("cdc", decode_payment_udf(F.col(value_column)))
    return decoded.select(
        F.col("cdc.payment_id").alias("payment_id"),
        F.col("cdc.merchant_id").alias("merchant_id"),
        F.col("cdc.shopper_id").alias("shopper_id"),
        F.col("cdc.amount_str").cast("decimal(12,2)").alias("amount"),
        F.col("cdc.currency").alias("currency"),
        F.col("cdc.payment_method").alias("payment_method"),
        F.col("cdc.payment_status").alias("payment_status"),
        F.col("cdc.country_code").alias("country_code"),
        F.col("cdc.created_at").alias("created_at"),
        F.col("cdc.updated_at").alias("updated_at"),
        F.col("cdc.cdc_op").alias("cdc_op"),
        F.col("cdc.cdc_lsn").alias("cdc_lsn"),
        F.col("cdc.cdc_source_ts").alias("cdc_source_ts"),
        F.col("cdc.is_deleted").alias("is_deleted"),
        F.col("cdc.is_tombstone").alias("is_tombstone"),
        F.col("cdc.decode_ok").alias("decode_ok"),
        F.current_timestamp().alias("ingested_at"),
    )


def latest_change_per_key(decoded: DataFrame, key: str = "payment_id") -> DataFrame:
    """Collapse a micro-batch to the final state of each row.

    Iceberg's MERGE raises if the source contains more than one row per target
    match -- an intentional guard against a non-deterministic update. A CDC
    batch routinely holds several changes to one payment, so it must be
    deduplicated *before* the MERGE, ordered by LSN.

    LSN is the authority. ``cdc_source_ts`` has millisecond resolution, so two
    changes inside one millisecond tie and a tie broken the wrong way applies a
    stale image over a fresh one.
    """
    ordering = F.struct(
        F.coalesce(F.col("cdc_lsn"), F.lit(-1)).alias("lsn"),
        F.coalesce(F.col("cdc_source_ts").cast("long"), F.lit(-1)).alias("ts"),
    )
    ranked = (
        decoded.where(F.col("decode_ok") & ~F.col("is_tombstone"))
        .withColumn("_ord", ordering)
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy(key).orderBy(F.col("_ord").desc())
            ),
        )
    )
    return ranked.where(F.col("_rank") == 1).drop("_ord", "_rank")


def gold_daily_merchant_revenue(silver: DataFrame) -> DataFrame:
    """Daily revenue per merchant, in original currency.

    Only settled money counts: authorized and captured payments are revenue,
    while failed, cancelled, pending and chargeback are not. Counting them is
    the most common way a payments dashboard overstates income.
    """
    settled = silver.where(F.col("payment_status").isin("authorized", "captured"))
    return (
        settled.groupBy(
            F.to_date("created_at").alias("revenue_date"),
            "merchant_id",
            "currency",
        )
        .agg(
            F.count("*").alias("payment_count"),
            F.sum("amount").alias("gross_amount"),
            F.avg("amount").cast("decimal(12,2)").alias("average_amount"),
            F.countDistinct("shopper_id").alias("distinct_shoppers"),
        )
    )


def gold_payment_method_mix(silver: DataFrame) -> DataFrame:
    """Share of payment volume by method, per day and country."""
    per_country_day = Window.partitionBy("activity_date", "country_code")
    return (
        silver.groupBy(
            F.to_date("created_at").alias("activity_date"),
            "country_code",
            "payment_method",
        )
        .agg(F.count("*").alias("payment_count"))
        .withColumn(
            "share_pct",
            F.round(
                100 * F.col("payment_count") / F.sum("payment_count").over(per_country_day),
                2,
            ),
        )
    )


def quarantine(decoded: DataFrame) -> DataFrame:
    """Records that could not be decoded, kept with their raw payload."""
    return decoded.where(~F.col("decode_ok") & ~F.col("is_tombstone"))
