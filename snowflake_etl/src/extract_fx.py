"""Fetch daily FX rates and land them for loading.

Written against a plain HTTP interface so the fetch function can be tested with
a stub transport: the parsing and gap-detection rules matter more than the
provider, and a test that needs the network is a test that fails on a plane.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from payments_platform.fx import FxRate, parse_rates_response  # noqa: E402

Fetcher = Callable[[str], Mapping]


def _http_get(url: str) -> Mapping:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def fetch_day(
    day: dt.date,
    base: str,
    currencies: Iterable[str],
    base_url: str,
    fetcher: Optional[Fetcher] = None,
) -> List[FxRate]:
    """Fetch one day's rates."""
    symbols = ",".join(sorted(currencies))
    url = f"{base_url}/{day.isoformat()}?base={base}&symbols={symbols}"
    payload = (fetcher or _http_get)(url)
    return parse_rates_response(payload)


def fetch_range(
    start: dt.date,
    end: dt.date,
    base: str,
    currencies: Iterable[str],
    base_url: str,
    fetcher: Optional[Fetcher] = None,
) -> List[FxRate]:
    """Fetch an inclusive date range, skipping days the provider has no data for.

    A missing day is normal -- the FX market is closed at weekends -- so it is
    not an error here. Downstream, ``int_fx_rates_daily`` carries the previous
    rate forward across the gap.
    """
    rates: List[FxRate] = []
    day = start
    while day <= end:
        rates.extend(fetch_day(day, base, currencies, base_url, fetcher))
        day += dt.timedelta(days=1)
    return rates


def to_rows(rates: Iterable[FxRate]) -> List[Dict[str, str]]:
    """Serialise rates for landing, with the rate as a string.

    Deliberately not a float: the landing file is the boundary where precision
    is easiest to lose, and every consumer casts to DECIMAL from here.
    """
    return [
        {
            "rate_date": rate.rate_date.isoformat(),
            "base_currency": rate.base,
            "currency": rate.currency,
            "rate": str(rate.rate),
        }
        for rate in rates
    ]


def write_landing_file(rates: Iterable[FxRate], destination: Path) -> Path:
    """Write newline-delimited JSON, the format Snowflake's COPY INTO expects."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        for row in to_rows(rates):
            handle.write(json.dumps(row) + "\n")
    return destination
