"""Shared fixtures: a local Spark session with a real Iceberg catalog."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

#: Spark's Python workers are separate processes and do not inherit the driver's
#: sys.path, so a UDF's module has to reach them through the environment.
os.environ["PYTHONPATH"] = os.pathsep.join(
    [str(ROOT / "src"), str(ROOT), os.environ.get("PYTHONPATH", "")]
)

ICEBERG_JAR = os.environ.get("ICEBERG_JAR", "")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark session with an Iceberg Hadoop catalog in a temp warehouse.

    A real catalog, not a mock: the MERGE semantics this project depends on
    (merge-on-read deletes, the multiple-source-match guard) are Iceberg's, and
    testing them against plain DataFrames would prove nothing about them.
    """
    pyspark = pytest.importorskip("pyspark")
    if not ICEBERG_JAR or not Path(ICEBERG_JAR).exists():
        pytest.skip("ICEBERG_JAR not available")

    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("payments-platform-tests")
        .config("spark.jars", ICEBERG_JAR)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
