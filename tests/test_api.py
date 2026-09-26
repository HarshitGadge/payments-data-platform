"""Serving-tier tests against a real SQL engine holding real gold rows.

DuckDB stands in for Trino: both speak ANSI SQL with ``?`` parameters, so the
same queries run unchanged. Using a real engine rather than a mock means these
tests would catch a broken query, which a mock repository cannot.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from api.main import create_app  # noqa: E402
from api.repository import SqlRepository  # noqa: E402


@pytest.fixture
def client():
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA gold")
    connection.execute(
        """
        CREATE TABLE gold.daily_merchant_revenue (
            revenue_date DATE, merchant_id BIGINT, currency VARCHAR,
            payment_count BIGINT, gross_amount DECIMAL(12,2),
            average_amount DECIMAL(12,2), distinct_shoppers BIGINT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gold.daily_merchant_revenue VALUES
            ('2026-01-01', 1, 'EUR', 3, 1234567.89, 411522.63, 3),
            ('2026-01-01', 1, 'USD',  2,    500.00,    250.00, 2),
            ('2026-01-02', 1, 'EUR', 1,      0.10,      0.10, 1),
            ('2026-01-02', 2, 'GBP', 5,    999.99,    200.00, 4)
        """
    )
    connection.execute(
        """
        CREATE TABLE gold.payment_method_mix (
            activity_date DATE, country_code VARCHAR,
            payment_method VARCHAR, payment_count BIGINT, share_pct DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gold.payment_method_mix VALUES
            ('2026-01-01', 'NL', 'card',   70, 70.0),
            ('2026-01-01', 'NL', 'paypal', 30, 30.0),
            ('2026-01-01', 'US', 'card',  100, 100.0)
        """
    )
    return TestClient(create_app(SqlRepository(connection)))


class TestHealth:
    def test_reports_ok_when_gold_is_reachable(self, client):
        body = client.get("/health").json()
        assert body["status"] == "ok" and body["gold_tables_reachable"] is True

    def test_returns_503_when_gold_is_missing(self):
        broken = duckdb.connect(":memory:")  # no gold schema at all
        response = TestClient(create_app(SqlRepository(broken))).get("/health")
        # 503, not 500: the dependency is down, the service itself is healthy.
        assert response.status_code == 503
        assert response.json()["status"] == "degraded"


class TestMerchantRevenue:
    def test_returns_days_for_the_requested_range(self, client):
        body = client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-02"},
        ).json()
        assert len(body["days"]) == 3
        assert {d["currency"] for d in body["days"]} == {"EUR", "USD"}

    def test_money_is_serialised_as_string_not_float(self, client):
        raw = client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-01"},
        ).text
        # A JSON number is a double in every mainstream client: 1234567.89
        # would come back as 1234567.8899999999. As a string it survives.
        assert '"gross_amount":"1234567.89"' in raw
        assert "1234567.8899" not in raw

    def test_small_amounts_keep_their_cents(self, client):
        body = client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-02", "end_date": "2026-01-02"},
        ).json()
        assert body["days"][0]["gross_amount"] == "0.10"

    def test_totals_are_per_currency_never_summed_across(self, client):
        body = client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-02"},
        ).json()
        totals = {m["currency"]: m["amount"] for m in body["total_by_currency"]}
        # EUR: 1234567.89 + 0.10. USD stays separate -- adding them is meaningless.
        assert totals == {"EUR": "1234567.99", "USD": "500.00"}

    def test_range_excludes_days_outside_it(self, client):
        body = client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-01"},
        ).json()
        assert {d["revenue_date"] for d in body["days"]} == {"2026-01-01"}

    def test_unknown_merchant_is_404(self, client):
        assert client.get(
            "/v1/merchants/999/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-02"},
        ).status_code == 404

    def test_reversed_date_range_is_422(self, client):
        assert client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2026-01-02", "end_date": "2026-01-01"},
        ).status_code == 422

    def test_excessive_range_is_rejected(self, client):
        assert client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "2020-01-01", "end_date": "2026-01-01"},
        ).status_code == 422

    def test_non_integer_merchant_id_is_422(self, client):
        assert client.get(
            "/v1/merchants/not-a-number/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-02"},
        ).status_code == 422

    def test_malformed_date_is_422(self, client):
        assert client.get(
            "/v1/merchants/1/revenue",
            params={"start_date": "yesterday", "end_date": "2026-01-02"},
        ).status_code == 422


class TestInjectionSafety:
    def test_sql_metacharacters_in_path_do_not_execute(self, client):
        # Parameterised queries: this is a bad integer, not a statement.
        response = client.get(
            "/v1/merchants/1 OR 1=1; DROP TABLE gold.daily_merchant_revenue--/revenue",
            params={"start_date": "2026-01-01", "end_date": "2026-01-02"},
        )
        assert response.status_code == 422

    def test_table_survives_an_injection_attempt(self, client):
        client.get(
            "/v1/payment-methods/2026-01-01",
            params={"country_code": "'; DROP TABLE gold.payment_method_mix--"},
        )
        # Rejected by validation, and the table is still queryable.
        assert client.get("/v1/payment-methods/2026-01-01").status_code == 200


class TestMethodMix:
    def test_returns_all_countries_when_unfiltered(self, client):
        body = client.get("/v1/payment-methods/2026-01-01").json()
        assert sum(m["payment_count"] for m in body["methods"]) == 200

    def test_filters_by_country(self, client):
        body = client.get(
            "/v1/payment-methods/2026-01-01", params={"country_code": "NL"}
        ).json()
        assert {m["payment_method"] for m in body["methods"]} == {"card", "paypal"}

    def test_country_filter_is_case_insensitive(self, client):
        body = client.get(
            "/v1/payment-methods/2026-01-01", params={"country_code": "nl"}
        ).json()
        assert body["country_code"] == "NL" and len(body["methods"]) == 2

    def test_ordered_by_volume_descending(self, client):
        body = client.get(
            "/v1/payment-methods/2026-01-01", params={"country_code": "NL"}
        ).json()
        assert [m["payment_method"] for m in body["methods"]] == ["card", "paypal"]

    def test_day_with_no_activity_returns_empty_not_404(self, client):
        body = client.get("/v1/payment-methods/2026-06-01").json()
        assert body["methods"] == []
