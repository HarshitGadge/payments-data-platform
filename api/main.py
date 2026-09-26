"""FastAPI application serving the gold lakehouse tables.

Built through a factory taking a :class:`~api.repository.Repository`, so the
same app object is served over Trino in a deployment and over DuckDB in tests.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import Decimal
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response

from .models import (
    DailyRevenue,
    HealthResponse,
    MerchantRevenueResponse,
    MethodMixResponse,
    Money,
    PaymentMethodShare,
)
from .repository import Repository, to_decimal

#: Upper bound on rows returned by one revenue request. A merchant with a long
#: history and six currencies can otherwise ask for a response large enough to
#: put the API under memory pressure.
MAX_DAYS = 366
DEFAULT_LIMIT = 1000


def create_app(repository: Repository) -> FastAPI:
    app = FastAPI(
        title="Payments Platform API",
        version="1.0.0",
        summary="Read-only serving tier over the gold payments tables.",
    )

    def get_repository() -> Repository:
        return repository

    @app.get("/health", response_model=HealthResponse, tags=["ops"])
    def health(response: Response, repo: Repository = Depends(get_repository)):
        reachable, detail = repo.health()
        if not reachable:
            # 503, not 500: the API is fine, its dependency is not. A load
            # balancer should take this instance out of rotation, not page.
            response.status_code = 503
        return HealthResponse(
            status="ok" if reachable else "degraded",
            gold_tables_reachable=reachable,
            detail=detail,
        )

    @app.get(
        "/v1/merchants/{merchant_id}/revenue",
        response_model=MerchantRevenueResponse,
        tags=["revenue"],
    )
    def merchant_revenue(
        merchant_id: int,
        start_date: dt.date = Query(..., description="Inclusive first revenue date"),
        end_date: dt.date = Query(..., description="Inclusive last revenue date"),
        limit: int = Query(DEFAULT_LIMIT, ge=1, le=10_000),
        repo: Repository = Depends(get_repository),
    ):
        if end_date < start_date:
            raise HTTPException(422, "end_date must not precede start_date")
        if (end_date - start_date).days > MAX_DAYS:
            raise HTTPException(422, f"date range must not exceed {MAX_DAYS} days")
        if not repo.merchant_exists(merchant_id):
            raise HTTPException(404, f"no revenue recorded for merchant {merchant_id}")

        rows = repo.daily_revenue(merchant_id, start_date, end_date, limit)
        days = [
            DailyRevenue(
                revenue_date=row[0],
                merchant_id=row[1],
                currency=row[2],
                payment_count=row[3],
                gross_amount=to_decimal(row[4]),
                average_amount=to_decimal(row[5]),
                distinct_shoppers=row[6],
            )
            for row in rows
        ]

        # Totals are summed per currency and never across currencies: adding
        # EUR to USD produces a number that means nothing.
        totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
        for day in days:
            totals[day.currency] += day.gross_amount

        return MerchantRevenueResponse(
            merchant_id=merchant_id,
            start_date=start_date,
            end_date=end_date,
            currencies=sorted(totals),
            total_by_currency=[
                Money(amount=amount, currency=currency)
                for currency, amount in sorted(totals.items())
            ],
            days=days,
        )

    @app.get(
        "/v1/payment-methods/{activity_date}",
        response_model=MethodMixResponse,
        tags=["mix"],
    )
    def method_mix(
        activity_date: dt.date,
        country_code: Optional[str] = Query(None, min_length=2, max_length=2),
        repo: Repository = Depends(get_repository),
    ):
        rows = repo.method_mix(activity_date, country_code.upper() if country_code else None)
        return MethodMixResponse(
            activity_date=activity_date,
            country_code=country_code.upper() if country_code else None,
            methods=[
                PaymentMethodShare(
                    payment_method=row[0], payment_count=row[1], share_pct=float(row[2])
                )
                for row in rows
            ],
        )

    return app
