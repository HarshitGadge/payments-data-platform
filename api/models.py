"""Response models for the serving API.

Money is typed ``Decimal`` and serialised as a JSON **string**. A JSON number
is an IEEE 754 double in every mainstream client, so ``1234567.89`` sent as a
number can be read back as ``1234567.8899999999``. Sending the digits as a
string is the only way the amount that arrives is the amount that was stored.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class Money(BaseModel):
    """An amount with its currency. Never a bare float."""

    model_config = ConfigDict(frozen=True)

    amount: Decimal
    currency: str = Field(min_length=3, max_length=3)

    @field_serializer("amount")
    def _serialize_amount(self, value: Decimal) -> str:
        return f"{value:.2f}"


class DailyRevenue(BaseModel):
    revenue_date: dt.date
    merchant_id: int
    currency: str
    payment_count: int
    gross_amount: Decimal
    average_amount: Decimal
    distinct_shoppers: int

    @field_serializer("gross_amount", "average_amount")
    def _serialize_money(self, value: Decimal) -> str:
        return f"{value:.2f}"


class MerchantRevenueResponse(BaseModel):
    merchant_id: int
    start_date: dt.date
    end_date: dt.date
    currencies: List[str]
    total_by_currency: List[Money]
    days: List[DailyRevenue]


class PaymentMethodShare(BaseModel):
    payment_method: str
    payment_count: int
    share_pct: float


class MethodMixResponse(BaseModel):
    activity_date: dt.date
    country_code: Optional[str]
    methods: List[PaymentMethodShare]


class HealthResponse(BaseModel):
    status: str
    gold_tables_reachable: bool
    detail: Optional[str] = None
