"""Tests for the FX extract and the Snowflake load statements."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

import pytest

from snowflake_etl.src.extract_fx import (
    fetch_day,
    fetch_range,
    to_rows,
    write_landing_file,
)
from snowflake_etl.src.load_snowflake import render_statements

FRIDAY = dt.date(2026, 1, 2)
SATURDAY = dt.date(2026, 1, 3)


def stub_api(responses):
    """A fetcher returning canned payloads, keyed by the date in the URL."""

    def fetch(url: str):
        for day, payload in responses.items():
            if day.isoformat() in url:
                return payload
        return {}

    return fetch


class TestFetch:
    def test_parses_a_days_rates(self):
        fetcher = stub_api(
            {FRIDAY: {"base": "USD", "date": FRIDAY.isoformat(), "rates": {"EUR": "0.92"}}}
        )
        rates = fetch_day(FRIDAY, "USD", ["EUR"], "https://fx.test", fetcher)
        assert len(rates) == 1 and rates[0].rate == Decimal("0.92")

    def test_requests_the_symbols_it_was_asked_for(self):
        seen = {}

        def fetch(url):
            seen["url"] = url
            return {}

        fetch_day(FRIDAY, "USD", ["GBP", "EUR"], "https://fx.test", fetch)
        assert "symbols=EUR,GBP" in seen["url"] and "base=USD" in seen["url"]

    def test_closed_market_day_yields_no_rates_without_failing(self):
        # The provider returns nothing for a Saturday. That is normal, not an
        # error: the warehouse carries Friday's rate forward across the gap.
        fetcher = stub_api(
            {FRIDAY: {"base": "USD", "date": FRIDAY.isoformat(), "rates": {"EUR": "0.92"}}}
        )
        rates = fetch_range(FRIDAY, SATURDAY, "USD", ["EUR"], "https://fx.test", fetcher)
        assert [r.rate_date for r in rates] == [FRIDAY]


class TestLanding:
    def test_rate_is_written_as_a_string(self):
        rows = to_rows(
            [
                r
                for r in fetch_day(
                    FRIDAY,
                    "USD",
                    ["EUR"],
                    "https://fx.test",
                    stub_api({FRIDAY: {"base": "USD", "date": FRIDAY.isoformat(), "rates": {"EUR": "0.92"}}}),
                )
            ]
        )
        # A JSON float here would reintroduce the representation error the
        # pipeline exists to avoid.
        assert rows[0]["rate"] == "0.92" and isinstance(rows[0]["rate"], str)

    def test_writes_newline_delimited_json(self, tmp_path):
        rates = fetch_day(
            FRIDAY,
            "USD",
            ["EUR", "GBP"],
            "https://fx.test",
            stub_api({FRIDAY: {"base": "USD", "date": FRIDAY.isoformat(), "rates": {"EUR": "0.92", "GBP": "0.79"}}}),
        )
        path = write_landing_file(rates, tmp_path / "fx" / "2026-01-02.json")
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2
        assert {json.loads(line)["currency"] for line in lines} == {"EUR", "GBP"}


class TestSnowflakeStatements:
    @pytest.fixture
    def statements(self):
        return render_statements(
            "FX_STAGE", "fx/", {"database": "PAYMENTS", "schema": "RAW"}
        )

    def test_targets_the_configured_database_and_schema(self, statements):
        assert all("PAYMENTS.RAW" in s for s in statements)

    def test_rate_column_is_numeric_not_float(self, statements):
        # Comments are stripped first: the DDL's own comment explains why FLOAT
        # is wrong, and matching on that would make this assertion vacuous.
        ddl = "\n".join(
            line for line in statements[0].splitlines() if not line.strip().startswith("--")
        )
        assert "NUMBER(18, 8)" in ddl
        assert "FLOAT" not in ddl.upper()

    def test_load_merges_on_the_natural_key(self, statements):
        merge = statements[-1]
        # A restated rate must update in place, not append a second row for the
        # same day, which would fan out every payment joined to it.
        assert "MERGE INTO" in merge
        assert "WHEN MATCHED" in merge and "WHEN NOT MATCHED" in merge
        assert "RATE_DATE" in merge and "CURRENCY" in merge
