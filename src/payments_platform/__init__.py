"""Payments data platform: streaming CDC lakehouse and batch FX warehouse."""

from .cdc import ChangeEvent, latest_per_key, parse_envelope
from .fx import FxRate, RateTable, parse_rates_response

__all__ = [
    "ChangeEvent",
    "parse_envelope",
    "latest_per_key",
    "FxRate",
    "RateTable",
    "parse_rates_response",
]
__version__ = "0.1.0"
