"""Decoding against messages captured from a real Debezium connector.

The fixtures here were produced by running this repo's own docker-compose stack
-- Postgres 16 with wal_level=logical, Debezium 2.7 via pgoutput -- and
consuming the resulting topic. They are not hand-written, so they pin the
decoder to what the connector actually emits rather than to what its
documentation says it emits.

The values are checked against the source rows in Postgres:

    payment_id | amount | currency | created_at
    -----------+--------+----------+---------------------------
          1001 |  42.13 | EUR      | 2026-09-26 04:03:07.456703
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import pytest

from payments_platform.cdc import (
    decode_decimal,
    decode_micro_timestamp,
    latest_per_key,
    parse_envelope,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def snapshot() -> dict:
    return load("debezium_snapshot_real.json")


@pytest.fixture
def update() -> dict:
    return load("debezium_update_real.json")


@pytest.fixture
def delete() -> dict:
    return load("debezium_delete_real.json")


class TestRealWireFormat:
    def test_numeric_really_arrives_base64_encoded(self, snapshot):
        # The premise the whole decoder rests on. 42.13 on the wire is 'EHU=':
        # base64 of the unscaled integer 4213 as big-endian bytes.
        raw_amount = snapshot["after"]["amount"]
        assert isinstance(raw_amount, str)
        assert raw_amount == "EHU="

    def test_decoded_amount_matches_the_source_row(self, snapshot):
        assert decode_decimal(snapshot["after"]["amount"], 2) == Decimal("42.13")

    def test_timestamp_really_arrives_in_microseconds(self, snapshot):
        raw = snapshot["after"]["created_at"]
        # 16 digits. Milliseconds would be 13.
        assert len(str(raw)) == 16
        assert decode_micro_timestamp(raw) == dt.datetime(
            2026, 9, 26, 4, 3, 7, 456703, tzinfo=dt.timezone.utc
        )

    def test_snapshot_op_is_r_and_counts_as_an_upsert(self, snapshot):
        event = parse_envelope(snapshot)
        assert event.op == "r" and event.is_upsert


class TestRealChangeEvents:
    def test_update_carries_the_after_image(self, update):
        event = parse_envelope(update)
        assert event.op == "u"
        assert event.row["payment_status"] == "refunded"

    def test_delete_carries_the_before_image_with_columns(self, delete):
        # Only populated because the source tables are REPLICA IDENTITY FULL.
        # Under the default REPLICA IDENTITY, `before` holds the key alone and
        # every other column here would be null.
        event = parse_envelope(delete)
        assert event.is_delete
        assert event.row["payment_id"] == 1002
        assert event.row["payment_status"] == "failed"
        assert decode_decimal(event.row["amount"], 2) == Decimal("59.26")

    def test_real_lsns_order_snapshot_before_update(self, snapshot, update):
        assert parse_envelope(update).lsn > parse_envelope(snapshot).lsn

    def test_collapsing_real_events_keeps_the_newest(self, snapshot, update):
        # Same payment_id 1001, snapshot then update.
        winners = latest_per_key(
            [parse_envelope(snapshot), parse_envelope(update)], ["payment_id"]
        )
        assert len(winners) == 1
        assert winners[0].row["payment_status"] == "refunded"
