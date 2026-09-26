"""Gold aggregation tests on a real Spark session."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from payments_platform.transforms import (  # noqa: E402
    gold_daily_merchant_revenue,
    gold_payment_method_mix,
)

SCHEMA = (
    "payment_id long, merchant_id long, shopper_id long, amount decimal(12,2), "
    "currency string, payment_method string, payment_status string, "
    "country_code string, created_at timestamp"
)


@pytest.fixture
def silver(spark):
    day = dt.datetime(2026, 1, 2, 10, 0, 0)
    rows = [
        (1, 1, 501, Decimal("100.00"), "EUR", "card", "authorized", "NL", day),
        (2, 1, 502, Decimal("50.00"), "EUR", "card", "captured", "NL", day),
        (3, 1, 501, Decimal("25.00"), "EUR", "paypal", "failed", "NL", day),
        (4, 1, 503, Decimal("10.00"), "EUR", "card", "refunded", "NL", day),
        (5, 1, 504, Decimal("75.00"), "USD", "card", "authorized", "US", day),
        (6, 2, 505, Decimal("40.00"), "EUR", "paypal", "chargeback", "NL", day),
    ]
    return spark.createDataFrame(rows, SCHEMA)


class TestDailyRevenue:
    def test_only_settled_statuses_count_as_revenue(self, silver):
        row = (
            gold_daily_merchant_revenue(silver)
            .where("merchant_id = 1 AND currency = 'EUR'")
            .collect()[0]
        )
        # authorized 100 + captured 50. failed, refunded and chargeback are not
        # revenue -- counting them is how a payments dashboard overstates income.
        assert row.gross_amount == Decimal("150.00")
        assert row.payment_count == 2

    def test_currencies_are_never_summed_together(self, silver):
        rows = gold_daily_merchant_revenue(silver).where("merchant_id = 1").collect()
        by_currency = {r.currency: r.gross_amount for r in rows}
        assert by_currency == {"EUR": Decimal("150.00"), "USD": Decimal("75.00")}

    def test_merchant_with_no_settled_payments_is_absent(self, silver):
        # Merchant 2's only payment is a chargeback.
        assert gold_daily_merchant_revenue(silver).where("merchant_id = 2").count() == 0

    def test_distinct_shoppers_deduplicates(self, silver):
        row = (
            gold_daily_merchant_revenue(silver)
            .where("merchant_id = 1 AND currency = 'EUR'")
            .collect()[0]
        )
        assert row.distinct_shoppers == 2

    def test_amounts_stay_decimal(self, silver):
        row = gold_daily_merchant_revenue(silver).collect()[0]
        assert isinstance(row.gross_amount, Decimal)


class TestMethodMix:
    def test_shares_sum_to_one_hundred_per_country_day(self, silver):
        rows = gold_payment_method_mix(silver).where("country_code = 'NL'").collect()
        assert sum(r.share_pct for r in rows) == pytest.approx(100.0)

    def test_counts_all_statuses_not_just_settled(self, silver):
        # Method mix describes traffic, not revenue: a failed card payment is
        # still a card payment attempt.
        rows = {
            r.payment_method: r.payment_count
            for r in gold_payment_method_mix(silver).where("country_code = 'NL'").collect()
        }
        assert rows == {"card": 3, "paypal": 2}

    def test_countries_are_scoped_separately(self, silver):
        rows = gold_payment_method_mix(silver).where("country_code = 'US'").collect()
        assert len(rows) == 1 and rows[0].share_pct == pytest.approx(100.0)
