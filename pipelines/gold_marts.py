"""Gold: aggregate the silver payments table into serving marts.

Full recompute with INSERT OVERWRITE rather than an incremental append. A CDC
source mutates history -- a payment authorised last week can be refunded today,
which changes last week's revenue -- so an append-only gold layer drifts from
silver within days.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from payments_platform.config import IcebergConfig  # noqa: E402
from payments_platform.session import configure_session  # noqa: E402
from payments_platform.transforms import (  # noqa: E402
    gold_daily_merchant_revenue,
    gold_payment_method_mix,
)

from silver_merge import SILVER_TABLE  # noqa: E402

REVENUE_TABLE = "daily_merchant_revenue"
METHOD_MIX_TABLE = "payment_method_mix"


def main() -> None:
    iceberg = IcebergConfig()
    spark = configure_session(SparkSession.builder.getOrCreate(), iceberg)
    silver = spark.table(iceberg.table("silver", SILVER_TABLE))

    for name, frame in (
        (REVENUE_TABLE, gold_daily_merchant_revenue(silver)),
        (METHOD_MIX_TABLE, gold_payment_method_mix(silver)),
    ):
        target = iceberg.table("gold", name)
        frame.writeTo(target).createOrReplace()
        print(f"Gold rebuild complete -> {target}")


if __name__ == "__main__":
    main()
