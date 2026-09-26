"""Foreign-exchange rates and normalising payments to a reporting currency.

A payments business takes money in many currencies and reports in one. Getting
that conversion right is mostly about three rules that are easy to state and
easy to get wrong:

* **Convert at the rate as of the payment date**, not today's rate. Revaluing
  history at the current rate makes last quarter's numbers move every morning.
* **FX markets close.** There is no rate published for a Saturday, a Sunday or
  Christmas Day, but payments happen on those days. A missing rate must carry
  forward the last published one -- treating it as null drops the payment from
  the report, and defaulting it to 1.0 silently reports GBP as USD.
* **Money is decimal.** Binary floats cannot represent 0.10, and the error
  accumulates over millions of rows into a reconciliation break.

Conversion here is pure and total: given a rate table it needs no network, no
warehouse and no Spark, so every rule above is directly testable.
"""

from __future__ import annotations

import bisect
import datetime as dt
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

#: Currency everything is normalised to for reporting.
REPORTING_CURRENCY = "USD"

#: Minor-unit precision for the reporting currency.
_CENTS = Decimal("0.01")


@dataclass(frozen=True)
class FxRate:
    """One published rate: units of ``currency`` per one ``base``."""

    rate_date: dt.date
    base: str
    currency: str
    rate: Decimal


def parse_rates_response(payload: Mapping) -> List[FxRate]:
    """Parse a daily-rates REST response into :class:`FxRate` rows.

    Shape handled (the common one across free FX providers)::

        {"base": "USD", "date": "2026-01-15", "rates": {"EUR": 0.92, ...}}

    Values go through ``str()`` on the way into ``Decimal``: constructing a
    Decimal straight from a float carries the float's representation error in.
    """
    base = (payload.get("base") or REPORTING_CURRENCY).upper()
    raw_date = payload.get("date")
    if not raw_date:
        return []
    try:
        rate_date = dt.date.fromisoformat(str(raw_date))
    except ValueError:
        return []

    rates: List[FxRate] = []
    for currency, value in (payload.get("rates") or {}).items():
        if value is None:
            continue
        try:
            rate = Decimal(str(value))
        except Exception:
            continue
        if rate <= 0:
            continue  # a non-positive rate is bad data, not a cheap currency
        rates.append(
            FxRate(rate_date=rate_date, base=base, currency=currency.upper(), rate=rate)
        )
    return rates


class RateTable:
    """Rates indexed for as-of-date lookup with carry-forward.

    Built once per job and queried per row, so lookup is a binary search over a
    sorted date list rather than a scan.
    """

    def __init__(self, rates: Iterable[FxRate], base: str = REPORTING_CURRENCY):
        self.base = base.upper()
        self._by_currency: Dict[str, Tuple[List[dt.date], List[Decimal]]] = {}

        staged: Dict[str, Dict[dt.date, Decimal]] = {}
        for rate in rates:
            if rate.base.upper() != self.base:
                continue
            staged.setdefault(rate.currency.upper(), {})[rate.rate_date] = rate.rate

        for currency, by_date in staged.items():
            dates = sorted(by_date)
            self._by_currency[currency] = (dates, [by_date[d] for d in dates])

    @property
    def currencies(self) -> Sequence[str]:
        return sorted(self._by_currency)

    def rate_on(self, currency: str, as_of: dt.date) -> Optional[Decimal]:
        """Rate for ``currency`` effective on ``as_of``.

        Returns the most recent rate published on or before that date, which is
        what carries a Friday rate across the weekend. Returns ``None`` when the
        date precedes the first published rate -- there is nothing to carry
        forward from, and inventing one would be fabricating a number.
        """
        currency = currency.upper()
        if currency == self.base:
            return Decimal(1)

        entry = self._by_currency.get(currency)
        if entry is None:
            return None
        dates, values = entry

        position = bisect.bisect_right(dates, as_of)
        if position == 0:
            return None
        return values[position - 1]

    def to_base(
        self, amount: Decimal, currency: str, as_of: dt.date
    ) -> Optional[Decimal]:
        """Convert ``amount`` in ``currency`` to the base currency.

        The table holds units of *currency* per one *base*, so converting into
        the base divides. Returns ``None`` when no rate applies, so the caller
        decides whether that is a quarantine or a hard failure -- this function
        never guesses at 1.0.
        """
        if amount is None:
            return None
        rate = self.rate_on(currency, as_of)
        if rate is None or rate == 0:
            return None
        converted = Decimal(amount) / rate
        return converted.quantize(_CENTS, rounding=ROUND_HALF_EVEN)


def missing_rate_dates(
    table: RateTable, currency: str, start: dt.date, end: dt.date
) -> List[dt.date]:
    """Dates in the range with no rate published, for data-quality reporting.

    Weekend gaps are expected and carried forward; a long run of them is not,
    and is usually the first sign that a rate feed has quietly stopped.
    """
    entry = table._by_currency.get(currency.upper())
    published = set(entry[0]) if entry else set()
    day, missing = start, []
    while day <= end:
        if day not in published:
            missing.append(day)
        day += dt.timedelta(days=1)
    return missing
