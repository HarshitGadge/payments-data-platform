"""Tests for Debezium change-event decoding.

Each case is a way a payments CDC pipeline corrupts data when the envelope is
read naively: money decoded as a string, timestamps off by 1000x, deletes
dropped, tombstones double-counted, stale images overwriting fresh ones.
"""

from __future__ import annotations

import base64
import datetime as dt
from decimal import Decimal

import pytest

from payments_platform.cdc import (
    ChangeEvent,
    decode_decimal,
    decode_micro_timestamp,
    event_order,
    latest_per_key,
    parse_envelope,
)


def unscaled_b64(unscaled: int) -> str:
    """Encode an unscaled integer the way Debezium encodes NUMERIC."""
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(length, "big", signed=True)).decode()


def envelope(op="u", after=None, before=None, lsn=1000, ts_ms=1767225600000):
    return {
        "before": before,
        "after": after,
        "source": {"table": "payments", "lsn": lsn, "ts_ms": ts_ms, "db": "payments"},
        "op": op,
        "ts_ms": ts_ms,
    }


def payment_row(payment_id=1001, amount_unscaled=14999, status="authorized"):
    return {
        "payment_id": payment_id,
        "merchant_id": 1,
        "amount": unscaled_b64(amount_unscaled),
        "currency": "EUR",
        "payment_status": status,
        "created_at": 1767225600000000,  # microseconds
    }


class TestDecodeDecimal:
    def test_decodes_base64_unscaled_integer(self):
        # 149.99 at scale 2 is the unscaled integer 14999.
        assert decode_decimal(unscaled_b64(14999), scale=2) == Decimal("149.99")

    def test_returns_decimal_not_float(self):
        value = decode_decimal(unscaled_b64(10), scale=2)
        assert isinstance(value, Decimal)
        # 0.10 has no exact binary float representation; Decimal does.
        assert value == Decimal("0.10")

    def test_decodes_negative_amount(self):
        # A refund adjustment: two's complement, high bit set.
        assert decode_decimal(unscaled_b64(-2550), scale=2) == Decimal("-25.50")

    def test_large_amount_spanning_multiple_bytes(self):
        assert decode_decimal(unscaled_b64(123456789), scale=2) == Decimal("1234567.89")

    def test_accepts_plain_string_from_string_handling_mode(self):
        assert decode_decimal("149.99", scale=2) == Decimal("149.99")

    def test_accepts_int_from_connect_schema(self):
        assert decode_decimal(14999, scale=2) == Decimal("149.99")

    def test_none_stays_none(self):
        assert decode_decimal(None, scale=2) is None

    def test_summing_decimals_does_not_drift(self):
        # The reason money is not float: 0.1 * 3 != 0.3 in binary floating point.
        cents = [decode_decimal(unscaled_b64(10), 2) for _ in range(3)]
        assert sum(cents) == Decimal("0.30")


class TestDecodeTimestamp:
    def test_microseconds_not_milliseconds(self):
        # 1767225600000000 us = 2026-01-01T00:00:00Z. Read as millis it would be
        # over 50,000 years in the future; divided by 1000 out of habit, 1970.
        assert decode_micro_timestamp(1767225600000000) == dt.datetime(
            2026, 1, 1, tzinfo=dt.timezone.utc
        )

    def test_none_stays_none(self):
        assert decode_micro_timestamp(None) is None

    def test_garbage_returns_none_rather_than_raising(self):
        assert decode_micro_timestamp("not-a-number") is None


class TestParseEnvelope:
    def test_insert_uses_after_image(self):
        event = parse_envelope(envelope("c", after=payment_row()))
        assert event.is_upsert and not event.is_delete
        assert event.row["payment_id"] == 1001

    def test_snapshot_read_is_an_upsert(self):
        # op='r' is emitted for every existing row on first connector start.
        # Ignoring it leaves the target table empty after a backfill.
        assert parse_envelope(envelope("r", after=payment_row())).is_upsert

    def test_delete_takes_the_before_image(self):
        # after is null on a delete; the row's columns only exist in before.
        event = parse_envelope(envelope("d", after=None, before=payment_row()))
        assert event.is_delete
        assert event.row["payment_id"] == 1001

    def test_tombstone_is_flagged_not_treated_as_a_delete(self):
        event = parse_envelope(None)
        assert event.is_tombstone

    def test_unwraps_connect_schema_payload(self):
        wrapped = {"schema": {"type": "struct"}, "payload": envelope("c", after=payment_row())}
        assert parse_envelope(wrapped).row["payment_id"] == 1001

    def test_lsn_is_captured_for_ordering(self):
        assert parse_envelope(envelope("u", after=payment_row(), lsn=987654)).lsn == 987654

    @pytest.mark.parametrize("bad", [{}, {"op": "x"}, "not a mapping", []])
    def test_unrecognisable_message_returns_none(self, bad):
        assert parse_envelope(bad) is None


class TestLatestPerKey:
    def _event(self, payment_id, status, lsn, op="u"):
        return parse_envelope(
            envelope(op, after=payment_row(payment_id=payment_id, status=status), lsn=lsn)
        )

    def test_collapses_to_highest_lsn(self):
        events = [
            self._event(1001, "authorized", lsn=100),
            self._event(1001, "refunded", lsn=300),
            self._event(1001, "captured", lsn=200),
        ]
        winners = latest_per_key(events, ["payment_id"])
        assert len(winners) == 1
        assert winners[0].row["payment_status"] == "refunded"

    def test_out_of_order_arrival_does_not_apply_a_stale_image(self):
        # The newest event arrives first in the partition; LSN must still win.
        events = [self._event(1001, "refunded", lsn=300), self._event(1001, "authorized", lsn=100)]
        assert latest_per_key(events, ["payment_id"])[0].row["payment_status"] == "refunded"

    def test_distinct_rows_are_kept_separately(self):
        events = [self._event(1001, "authorized", 100), self._event(1002, "failed", 101)]
        assert len(latest_per_key(events, ["payment_id"])) == 2

    def test_delete_can_win_over_an_earlier_update(self):
        events = [
            self._event(1001, "authorized", lsn=100),
            parse_envelope(envelope("d", before=payment_row(1001), lsn=400)),
        ]
        assert latest_per_key(events, ["payment_id"])[0].is_delete

    def test_tombstones_are_excluded(self):
        events = [self._event(1001, "authorized", 100), parse_envelope(None)]
        assert len(latest_per_key(events, ["payment_id"])) == 1

    def test_lsn_takes_priority_over_millisecond_timestamp(self):
        # Two changes inside the same millisecond tie on ts_ms; LSN breaks it.
        old = parse_envelope(envelope("u", after=payment_row(status="authorized"), lsn=100, ts_ms=1767225600000))
        new = parse_envelope(envelope("u", after=payment_row(status="refunded"), lsn=101, ts_ms=1767225600000))
        assert event_order(new) > event_order(old)
