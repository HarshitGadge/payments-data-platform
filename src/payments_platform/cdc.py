"""Decoding Debezium change events from Postgres logical replication.

A Debezium envelope is not the row. It wraps the row in before/after images
plus source metadata, and it encodes several Postgres types in ways that are
lossy or simply wrong if read naively:

* ``NUMERIC`` columns arrive, under the default ``decimal.handling.mode=precise``,
  as a base64 string holding the *unscaled* value as big-endian two's-complement
  bytes. Read as a plain string, ``149.99`` becomes ``"A6y"``. Cast to float, it
  raises. This is the single most common way a payments CDC pipeline silently
  corrupts money.
* ``TIMESTAMP`` columns arrive as **microseconds** since epoch
  (``io.debezium.time.MicroTimestamp``), not milliseconds. Divided by 1000 out of
  habit, every event lands in 1970.
* A delete carries ``after: null`` and the row in ``before``. Reading only
  ``after`` drops deletes entirely, so rows resurrect in the warehouse.
* Deletes are followed by a **tombstone**: same key, ``value: null``. It is a log
  compaction marker, not a second delete, and counting it double-counts.

Everything here is pure Python over already-decoded JSON so the rules above can
be tested without Kafka, Spark or a database.
"""

from __future__ import annotations

import base64
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional

#: Debezium operation codes. ``r`` is a snapshot read, emitted for every existing
#: row when a connector first starts; it must be treated as an upsert, not
#: ignored, or the table starts empty.
OP_CREATE, OP_UPDATE, OP_DELETE, OP_READ, OP_TRUNCATE = "c", "u", "d", "r", "t"

UPSERT_OPS = frozenset({OP_CREATE, OP_UPDATE, OP_READ})

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def decode_decimal(value: Any, scale: int) -> Optional[Decimal]:
    """Decode a Debezium ``precise``-mode decimal.

    The wire form is base64 of the unscaled integer as big-endian two's
    complement, with the scale carried in the Connect schema. ``149.99`` at
    scale 2 is the unscaled integer ``14999``.

    ``Decimal`` is returned rather than ``float``: binary floating point cannot
    represent ``0.10`` exactly, and summing millions of payment amounts in
    float accumulates a drift that shows up as a reconciliation break.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int,)):
        return Decimal(value).scaleb(-scale)
    if isinstance(value, float):
        # decimal.handling.mode=double. Lossy, but the connector already made
        # that choice; go through str() so we don't compound the error.
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            raw = base64.b64decode(value, validate=True)
        except Exception:
            # decimal.handling.mode=string gives the plain digits.
            try:
                return Decimal(value)
            except Exception:
                return None
        if not raw:
            return None
        unscaled = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(unscaled).scaleb(-scale)
    return None


def decode_micro_timestamp(value: Any) -> Optional[dt.datetime]:
    """Decode ``io.debezium.time.MicroTimestamp`` (microseconds since epoch)."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    try:
        micros = int(value)
    except (TypeError, ValueError):
        return None
    return _EPOCH + dt.timedelta(microseconds=micros)


def decode_milli_timestamp(value: Any) -> Optional[dt.datetime]:
    """Decode ``io.debezium.time.Timestamp`` (milliseconds since epoch)."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    return _EPOCH + dt.timedelta(milliseconds=millis)


@dataclass(frozen=True)
class ChangeEvent:
    """One decoded Debezium change event.

    ``row`` is the state to apply: the ``after`` image for an upsert, the
    ``before`` image for a delete (the only place the deleted row's columns
    still exist).
    """

    op: str
    table: str
    row: Mapping[str, Any]
    lsn: Optional[int]
    source_ts: Optional[dt.datetime]
    is_tombstone: bool = False

    @property
    def is_delete(self) -> bool:
        return self.op == OP_DELETE

    @property
    def is_upsert(self) -> bool:
        return self.op in UPSERT_OPS

    def key(self, key_columns: Iterable[str]) -> tuple:
        """Primary-key tuple used to collapse events for the same row."""
        return tuple(self.row.get(column) for column in key_columns)


def parse_envelope(value: Optional[Mapping[str, Any]]) -> Optional[ChangeEvent]:
    """Decode one Debezium envelope into a :class:`ChangeEvent`.

    ``None`` (a Kafka record with a null value) is a tombstone: it is returned
    as an event flagged ``is_tombstone`` so a caller can count it, rather than
    being confused with a delete that carries data.

    Returns ``None`` for a record that is not a recognisable envelope, so a
    malformed message quarantines one row instead of failing a batch.
    """
    if value is None:
        return ChangeEvent(
            op=OP_DELETE, table="", row={}, lsn=None, source_ts=None, is_tombstone=True
        )
    if not isinstance(value, Mapping):
        return None

    # Some connector configurations wrap the envelope in a Connect schema.
    payload = value.get("payload") if "payload" in value else value
    if not isinstance(payload, Mapping):
        return None

    op = payload.get("op")
    if op not in {OP_CREATE, OP_UPDATE, OP_DELETE, OP_READ, OP_TRUNCATE}:
        return None

    source = payload.get("source") or {}
    row = payload.get("before") if op == OP_DELETE else payload.get("after")
    if row is None:
        row = payload.get("before") or {}

    lsn = source.get("lsn")
    return ChangeEvent(
        op=op,
        table=source.get("table") or "",
        row=row,
        lsn=int(lsn) if isinstance(lsn, (int, str)) and str(lsn).isdigit() else None,
        source_ts=decode_milli_timestamp(source.get("ts_ms")),
    )


def event_order(event: ChangeEvent) -> tuple:
    """Sort key establishing which of two events for a row happened last.

    LSN is the authority -- it is Postgres's own write-ahead-log position and is
    strictly increasing. ``ts_ms`` is only a fallback: it has millisecond
    resolution, so two changes to the same row inside one millisecond tie, and a
    tie broken the wrong way applies a stale image over a newer one.
    """
    return (
        event.lsn if event.lsn is not None else -1,
        event.source_ts.timestamp() if event.source_ts else -1.0,
    )


def latest_per_key(
    events: Iterable[ChangeEvent], key_columns: Iterable[str]
) -> List[ChangeEvent]:
    """Collapse a batch to the final state of each row.

    A micro-batch routinely contains several changes to the same payment
    (authorized then captured then refunded). Applying them as an unordered
    MERGE lets whichever arrived last in the partition win, which is not the
    same as whichever happened last. Collapsing by LSN first makes the MERGE
    deterministic and cuts the write volume.
    """
    key_columns = list(key_columns)
    winners: Dict[tuple, ChangeEvent] = {}
    for event in events:
        if event.is_tombstone:
            continue  # compaction marker; the delete itself already applied
        key = event.key(key_columns)
        if any(part is None for part in key):
            continue
        current = winners.get(key)
        if current is None or event_order(event) >= event_order(current):
            winners[key] = event
    return list(winners.values())
