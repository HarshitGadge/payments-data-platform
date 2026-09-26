"""Load landed FX rates into Snowflake.

Uses COPY INTO from an external stage rather than row-by-row INSERTs: COPY is
bulk-parallel and, because it tracks which files it has already loaded, is
naturally idempotent on re-run.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from payments_platform.config import SnowflakeConfig  # noqa: E402

CREATE_RAW_FX = """
CREATE TABLE IF NOT EXISTS {database}.{schema}.RAW_FX_RATES (
    RATE_DATE      DATE,
    BASE_CURRENCY  VARCHAR(3),
    CURRENCY       VARCHAR(3),
    -- NUMBER, never FLOAT: a rate stored as a float reintroduces the
    -- representation error the pipeline works to avoid.
    RATE           NUMBER(18, 8),
    LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
"""

COPY_FX = """
COPY INTO {database}.{schema}.RAW_FX_RATES
    (RATE_DATE, BASE_CURRENCY, CURRENCY, RATE)
FROM (
    SELECT $1:rate_date::DATE,
           $1:base_currency::VARCHAR,
           $1:currency::VARCHAR,
           $1:rate::NUMBER(18,8)
    FROM @{stage}/{prefix}
)
FILE_FORMAT = (TYPE = JSON)
-- Skip files already loaded, so a retried task does not duplicate rows.
ON_ERROR = ABORT_STATEMENT
"""

#: Rates are restated when a provider corrects a published value, so the load
#: is a MERGE on the natural key rather than a blind append.
DEDUPLICATE_FX = """
MERGE INTO {database}.{schema}.FX_RATES AS target
USING (
    SELECT RATE_DATE, BASE_CURRENCY, CURRENCY, RATE,
           ROW_NUMBER() OVER (
               PARTITION BY RATE_DATE, BASE_CURRENCY, CURRENCY
               ORDER BY LOADED_AT DESC
           ) AS recency
    FROM {database}.{schema}.RAW_FX_RATES
) AS source
ON  target.RATE_DATE     = source.RATE_DATE
AND target.BASE_CURRENCY = source.BASE_CURRENCY
AND target.CURRENCY      = source.CURRENCY
WHEN MATCHED AND source.recency = 1 THEN UPDATE SET target.RATE = source.RATE
WHEN NOT MATCHED AND source.recency = 1 THEN INSERT
    (RATE_DATE, BASE_CURRENCY, CURRENCY, RATE)
    VALUES (source.RATE_DATE, source.BASE_CURRENCY, source.CURRENCY, source.RATE)
"""


def render_statements(stage: str, prefix: str, settings: Mapping[str, str]):
    """Render the load statements for the configured target.

    Returned rather than executed so they can be inspected in a test without a
    Snowflake connection.
    """
    context = {
        "database": settings["database"],
        "schema": settings["schema"],
        "stage": stage,
        "prefix": prefix,
    }
    return [
        CREATE_RAW_FX.format(**context),
        COPY_FX.format(**context),
        DEDUPLICATE_FX.format(**context),
    ]


def main() -> None:
    import snowflake.connector  # imported lazily; tests need no driver

    settings = SnowflakeConfig.from_env()
    statements = render_statements(
        stage=sys.argv[1] if len(sys.argv) > 1 else "FX_STAGE",
        prefix=sys.argv[2] if len(sys.argv) > 2 else "fx/",
        settings=settings,
    )
    connection = snowflake.connector.connect(**settings)
    try:
        for statement in statements:
            connection.cursor().execute(statement)
        print(f"loaded {len(statements)} statements into {settings['database']}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
