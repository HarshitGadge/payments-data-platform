"""ASGI entry point: ``uvicorn api.app:app``.

Wires the application to Trino over the Iceberg REST catalog. Import-time
connection so a misconfigured deployment fails at startup rather than on the
first request.
"""

from __future__ import annotations

import os

from .main import create_app
from .repository import SqlRepository


def _connection():
    import trino  # imported here so tests need no Trino client installed

    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "payments-api"),
        catalog=os.environ.get("TRINO_CATALOG", "lakehouse"),
        schema=os.environ.get("TRINO_SCHEMA", "gold"),
    )


app = create_app(
    SqlRepository(
        _connection(),
        revenue_table=os.environ.get("REVENUE_TABLE", "lakehouse.gold.daily_merchant_revenue"),
        method_table=os.environ.get("METHOD_TABLE", "lakehouse.gold.payment_method_mix"),
    )
)
