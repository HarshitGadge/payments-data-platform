"""Platform configuration: Kafka, Iceberg, Snowflake and the serving API.

Values come from the environment so the same code runs under Compose, on
Kubernetes and against a cloud warehouse without edits. Nothing here holds a
credential default -- a missing secret raises rather than silently connecting
somewhere unintended.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, Optional


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"required environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class KafkaConfig:
    """Source topic carrying Debezium change events."""

    bootstrap_servers: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    payments_topic: str = "cdc.public.payments"
    merchants_topic: str = "cdc.public.merchants"
    refunds_topic: str = "cdc.public.refunds"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 100_000

    def source_options(self, topic: str) -> Dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": self.starting_offsets,
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
            # A compacted CDC topic can drop offsets out from under the reader;
            # that is a reason to re-snapshot, not to crash the job.
            "failOnDataLoss": "false",
        }


@dataclass(frozen=True)
class IcebergConfig:
    """Iceberg catalog holding the medallion tables."""

    catalog: str = field(default_factory=lambda: os.environ.get("ICEBERG_CATALOG", "lakehouse"))
    warehouse: str = field(default_factory=lambda: os.environ.get("ICEBERG_WAREHOUSE", "s3://lakehouse/warehouse"))
    rest_uri: str = field(default_factory=lambda: os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
    bronze_namespace: str = "bronze"
    silver_namespace: str = "silver"
    gold_namespace: str = "gold"

    def table(self, layer: str, name: str) -> str:
        namespace = getattr(self, f"{layer}_namespace")
        return f"{self.catalog}.{namespace}.{name}"

    def spark_conf(self) -> Dict[str, str]:
        """Catalog settings a Spark job needs to reach these tables."""
        prefix = f"spark.sql.catalog.{self.catalog}"
        return {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": "rest",
            f"{prefix}.uri": self.rest_uri,
            f"{prefix}.warehouse": self.warehouse,
            f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        }


@dataclass(frozen=True)
class SnowflakeConfig:
    """Target for the batch FX warehouse.

    Read from the environment at call time rather than import time so that
    importing this module in a test or a Spark job does not require Snowflake
    credentials to exist.
    """

    @staticmethod
    def from_env() -> Dict[str, str]:
        return {
            "account": _env("SNOWFLAKE_ACCOUNT"),
            "user": _env("SNOWFLAKE_USER"),
            "password": _env("SNOWFLAKE_PASSWORD"),
            "role": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "PAYMENTS"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        }


@dataclass(frozen=True)
class FxConfig:
    """Daily FX rate feed for the batch pipeline."""

    base_url: str = field(default_factory=lambda: os.environ.get("FX_API_URL", "https://api.exchangerate.host"))
    base_currency: str = "USD"
    currencies: tuple = ("EUR", "GBP", "CAD", "AUD", "CHF")
    landing_prefix: str = field(default_factory=lambda: os.environ.get("FX_LANDING_PREFIX", "s3://payments-raw/fx"))


#: Primary keys used to collapse CDC events and to MERGE into silver.
PRIMARY_KEYS = {
    "payments": ("payment_id",),
    "merchants": ("merchant_id",),
    "refunds": ("refund_id",),
}
