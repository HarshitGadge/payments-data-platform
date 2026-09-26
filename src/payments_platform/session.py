"""Spark session settings the medallion tables depend on."""

from __future__ import annotations

from .config import IcebergConfig

#: All source timestamps are UTC. Spark renders timestamps and derives calendar
#: parts in spark.sql.session.timeZone, which defaults to the JVM's local zone,
#: so an unpinned cluster produces different daily aggregates for the same data.
SESSION_TIMEZONE = "UTC"


def configure_session(spark, iceberg: IcebergConfig | None = None):
    """Apply the settings the pipelines assume. Returns the session."""
    spark.conf.set("spark.sql.session.timeZone", SESSION_TIMEZONE)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # Iceberg MERGE rewrites only the affected files when this is on; without it
    # a small CDC batch rewrites whole partitions.
    spark.conf.set("spark.sql.iceberg.merge-schema", "true")
    for key, value in (iceberg or IcebergConfig()).spark_conf().items():
        spark.conf.set(key, value)
    return spark
