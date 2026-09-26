"""Tests for FX rate lookup and currency normalisation.

The cases here are the ways cross-currency revenue reporting goes wrong:
converting at today's rate instead of the payment date's, dropping or
mis-defaulting payments made on days the FX market was closed, and losing cents
to binary floating point.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from payments_platform.fx import (
    RateTable,
    missing_rate_dates,
    parse_rates_response,
)

FRIDAY = dt.date(2026, 1, 2)
SATURDAY = dt.date(2026, 1, 3)
SUNDAY = dt.date(2026, 1, 4)
MONDAY = dt.date(2026, 1, 5)


def response(date, **rates):
    return {"base": "USD", "date": date.isoformat(), "rates": rates}


@pytest.fixture
def table() -> RateTable:
    rates = []
    # EUR strengthens over the weekend; GBP only published on the Friday.
    rates += parse_rates_response(response(FRIDAY, EUR="0.92", GBP="0.79", JPY="157.2"))
    rates += parse_rates_response(response(MONDAY, EUR="0.95", JPY="158.0"))
    return RateTable(rates)


class TestParseRatesResponse:
    def test_parses_each_currency(self):
        rates = parse_rates_response(response(FRIDAY, EUR="0.92", GBP="0.79"))
        assert {r.currency for r in rates} == {"EUR", "GBP"}

    def test_values_are_decimal_not_float(self):
        rate = parse_rates_response(response(FRIDAY, EUR=0.92))[0].rate
        assert isinstance(rate, Decimal)
        # Decimal(0.92) would carry the float's representation error; str() first.
        assert rate == Decimal("0.92")

    def test_null_and_non_positive_rates_are_discarded(self):
        rates = parse_rates_response(response(FRIDAY, EUR=None, GBP="0", CHF="-1", JPY="157"))
        assert {r.currency for r in rates} == {"JPY"}

    def test_malformed_date_yields_nothing(self):
        assert parse_rates_response({"base": "USD", "date": "not-a-date", "rates": {"EUR": 1}}) == []


class TestRateLookup:
    def test_uses_the_rate_as_of_the_payment_date(self, table):
        # Not today's rate: last quarter's numbers must not move overnight.
        assert table.rate_on("EUR", FRIDAY) == Decimal("0.92")
        assert table.rate_on("EUR", MONDAY) == Decimal("0.95")

    @pytest.mark.parametrize("weekend_day", [SATURDAY, SUNDAY])
    def test_weekend_carries_forward_fridays_rate(self, table, weekend_day):
        # FX markets close; payments do not stop.
        assert table.rate_on("EUR", weekend_day) == Decimal("0.92")

    def test_carry_forward_spans_a_long_gap(self, table):
        # GBP was published once, on the Friday.
        assert table.rate_on("GBP", dt.date(2026, 3, 1)) == Decimal("0.79")

    def test_reporting_currency_is_exactly_one(self, table):
        assert table.rate_on("USD", SATURDAY) == Decimal(1)

    def test_date_before_first_rate_has_no_rate(self, table):
        # Nothing to carry forward from; inventing a rate would fabricate money.
        assert table.rate_on("EUR", dt.date(2025, 12, 1)) is None

    def test_unknown_currency_has_no_rate(self, table):
        assert table.rate_on("XYZ", FRIDAY) is None

    def test_lookup_is_case_insensitive(self, table):
        assert table.rate_on("eur", FRIDAY) == Decimal("0.92")


class TestConversion:
    def test_converts_into_the_base_currency(self, table):
        # 0.92 EUR per USD, so 149.99 EUR is 149.99 / 0.92 USD.
        assert table.to_base(Decimal("149.99"), "EUR", FRIDAY) == Decimal("163.03")

    def test_usd_passes_through_unchanged(self, table):
        assert table.to_base(Decimal("499.50"), "USD", SATURDAY) == Decimal("499.50")

    def test_same_payment_converts_differently_on_different_dates(self, table):
        friday = table.to_base(Decimal("100.00"), "EUR", FRIDAY)
        monday = table.to_base(Decimal("100.00"), "EUR", MONDAY)
        assert friday != monday
        assert friday == Decimal("108.70") and monday == Decimal("105.26")

    def test_result_is_quantised_to_cents(self, table):
        assert table.to_base(Decimal("1.00"), "JPY", FRIDAY).as_tuple().exponent == -2

    def test_missing_rate_returns_none_rather_than_defaulting_to_one(self, table):
        # Defaulting to 1.0 would report 100 GBP as 100 USD.
        assert table.to_base(Decimal("100.00"), "GBP", dt.date(2025, 1, 1)) is None
        assert table.to_base(Decimal("100.00"), "XYZ", FRIDAY) is None

    def test_conversion_stays_decimal_end_to_end(self, table):
        assert isinstance(table.to_base(Decimal("0.10"), "EUR", FRIDAY), Decimal)

    def test_totals_do_not_drift_over_many_rows(self, table):
        # 300 payments of 0.10 USD is exactly 30.00, not 29.999999999999996.
        total = sum(table.to_base(Decimal("0.10"), "USD", FRIDAY) for _ in range(300))
        assert total == Decimal("30.00")


class TestDataQuality:
    def test_reports_dates_with_no_published_rate(self, table):
        missing = missing_rate_dates(table, "EUR", FRIDAY, MONDAY)
        # The weekend is expected; a long run of these means a dead feed.
        assert missing == [SATURDAY, SUNDAY]
