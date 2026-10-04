import struct

import pytest
import wal101
from wal101 import Col, RawTuple

INT4 = Col("x", 23, 4, "i", False)
INT8 = Col("x", 20, 8, "d", False)
TEXT = Col("name", 25, -1, "i", False)


def alice() -> RawTuple:
    # payload of the first INSERT in `just demo`: (1, 'alice', 10) with hoff=24 -> 1 byte of padding first
    return RawTuple(3, 0, 24, bytes.fromhex("00 01 00 00 00 0d 61 6c 69 63 65 00 00 0a 00 00 00"))


def cols(qty: Col) -> list[Col]:
    return [Col("id", 23, 4, "i", False), TEXT, Col("qty", qty.typid, qty.attlen, qty.align, False)]


def test_decodes_with_the_catalog_it_was_written_under() -> None:
    assert wal101.decode_tuple(alice(), cols(INT4)) == {"id": 1, "name": "alice", "qty": 10}


def test_stale_catalog_fails_loudly() -> None:
    with pytest.raises(ValueError, match="ran off the end"):
        wal101.decode_tuple(alice(), cols(INT8))


def test_null_bitmap_skips_columns() -> None:
    # natts=2, HEAP_HASNULL, bitmap 0b01 -> only column 0 present; hoff 24 -> bitmap byte + 0 pad
    t = RawTuple(2, 0x0001, 24, b"\x01" + struct.pack("<i", 7))
    assert wal101.decode_tuple(t, [Col("a", 23, 4, "i", False), Col("b", 23, 4, "i", False)]) == {"a": 7, "b": None}


def test_columns_added_later_read_as_null() -> None:
    t = RawTuple(1, 0, 24, b"\x00" + struct.pack("<i", 5))
    assert wal101.decode_tuple(t, [Col("a", 23, 4, "i", False), Col("added", 25, -1, "i", False)]) == {
        "a": 5,
        "added": None,
    }


def test_align() -> None:
    assert [wal101.align(n, 8) for n in (0, 1, 8, 9)] == [0, 8, 8, 16]
