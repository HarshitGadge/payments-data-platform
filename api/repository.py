"""Query layer for the serving API.

The API is deployed over Trino reading Iceberg, but the SQL it issues is plain
ANSI that DuckDB also runs. Keeping the queries behind this interface means the
endpoint logic is exercised in tests against a real SQL engine holding real
rows, rather than against a hand-written mock that always agrees with itself.

Every query is parameterised. String-formatting a merchant id or a date into
SQL is how a serving tier over a warehouse becomes an injection vector, and the
warehouse credential usually has far more than read access to one table.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any, List, Optional, Protocol, Sequence, Tuple

DAILY_REVENUE_SQL = """
SELECT revenue_date, merchant_id, currency, payment_count,
       gross_amount, average_amount, distinct_shoppers
FROM {table}
WHERE merchant_id = ?
  AND revenue_date >= ?
  AND revenue_date <= ?
ORDER BY revenue_date, currency
LIMIT ?
"""

METHOD_MIX_SQL = """
SELECT payment_method, payment_count, share_pct
FROM {table}
WHERE activity_date = ?
  AND (? IS NULL OR country_code = ?)
ORDER BY payment_count DESC
"""

MERCHANT_EXISTS_SQL = "SELECT 1 FROM {table} WHERE merchant_id = ? LIMIT 1"


class Repository(Protocol):
    """What the endpoints need from a warehouse."""

    def daily_revenue(
        self, merchant_id: int, start: dt.date, end: dt.date, limit: int
    ) -> List[Tuple]: ...

    def method_mix(
        self, activity_date: dt.date, country_code: Optional[str]
    ) -> List[Tuple]: ...

    def merchant_exists(self, merchant_id: int) -> bool: ...

    def health(self) -> Tuple[bool, Optional[str]]: ...


class SqlRepository:
    """Repository over any DB-API connection that speaks ``?`` parameters.

    Covers DuckDB directly; Trino's Python client uses the same paramstyle via
    ``trino.dbapi``. The table names are supplied by configuration, not by
    request data, so interpolating them is safe -- the *values* are always bound.
    """

    def __init__(
        self,
        connection: Any,
        revenue_table: str = "gold.daily_merchant_revenue",
        method_table: str = "gold.payment_method_mix",
    ):
        self._connection = connection
        self._revenue_table = revenue_table
        self._method_table = method_table

    def _query(self, sql: str, params: Sequence[Any]) -> List[Tuple]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, list(params))
            return [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def daily_revenue(
        self, merchant_id: int, start: dt.date, end: dt.date, limit: int
    ) -> List[Tuple]:
        return self._query(
            DAILY_REVENUE_SQL.format(table=self._revenue_table),
            [merchant_id, start, end, limit],
        )

    def method_mix(
        self, activity_date: dt.date, country_code: Optional[str]
    ) -> List[Tuple]:
        return self._query(
            METHOD_MIX_SQL.format(table=self._method_table),
            [activity_date, country_code, country_code],
        )

    def merchant_exists(self, merchant_id: int) -> bool:
        return bool(
            self._query(
                MERCHANT_EXISTS_SQL.format(table=self._revenue_table), [merchant_id]
            )
        )

    def health(self) -> Tuple[bool, Optional[str]]:
        try:
            self._query(f"SELECT 1 FROM {self._revenue_table} LIMIT 1", [])
            return True, None
        except Exception as exc:  # surfaced as a 503, not a 500 stack trace
            return False, type(exc).__name__


def to_decimal(value: Any) -> Decimal:
    """Coerce a warehouse numeric to Decimal without passing through float."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value if value is not None else 0))
