"""WAL 101: decode PostgreSQL *physical* WAL by hand, to see why walshadow needs a shadow catalog.

Runs one tiny workload and shows it three ways:

1. Logical decoding  - what the *source* does for you (test_decoding output)
2. Raw physical WAL  - what replicas (and walshadow) actually receive: bytes + a relfilenode, no names, no types
3. Catalog-aware decode - what walshadow does outside the source, using the schema *as of each record*

Then it decodes the same bytes with only the *current* catalog, to show why a static schema snapshot fails.
Targets PostgreSQL 17 WAL layout. Educational, not production: no TOAST, no FPI extraction, few types.
"""

import bisect
import os
import struct
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import LiteralString

import psycopg

DSN = os.environ.get("WAL101_DSN", "postgresql://postgres:postgres@localhost:5544/postgres")
CONTAINER = os.environ.get("WAL101_CONTAINER", "wal101-pg")
OUT_DIR = Path(__file__).resolve().parent.parent / ".wal-segments"

PAGE = 8192
SEG = 16 * 1024 * 1024
RM_XACT, RM_HEAP = 1, 10
HEAP_INSERT, HEAP_DELETE, HEAP_UPDATE, HEAP_HOT_UPDATE = 0x00, 0x10, 0x20, 0x40
XACT_COMMIT, XACT_ABORT = 0x00, 0x20
ALIGN = {"c": 1, "s": 2, "i": 4, "d": 8}
HEAP_HDR = 23  # fixed part of the heap tuple header, which WAL leaves out


def align(off: int, to: int) -> int:
    return (off + to - 1) & ~(to - 1)


# --------------------------------------------------------------------------- WAL segment -> records


@dataclass
class Block:
    rel: int
    blkno: int
    has_image: bool
    data: bytes


@dataclass
class Record:
    lsn: int
    xid: int
    rmid: int
    info: int
    blocks: list[Block]
    main: bytes


@dataclass
class Stream:
    """Page payloads of one segment glued together, so records can span page boundaries."""

    data: bytearray = field(default_factory=bytearray)
    page_starts: list[int] = field(default_factory=list)
    file_offsets: list[int] = field(default_factory=list)
    seg_start_lsn: int = 0

    def lsn(self, off: int) -> int:
        i = bisect.bisect_right(self.page_starts, off) - 1
        return self.seg_start_lsn + self.file_offsets[i] + (off - self.page_starts[i])


def read_segment(raw: bytes, seg_start_lsn: int) -> tuple[Stream, int]:
    s = Stream(seg_start_lsn=seg_start_lsn)
    skip = 0
    for n in range(SEG // PAGE):
        page = raw[n * PAGE : (n + 1) * PAGE]
        magic, info, _tli, _addr, rem_len = struct.unpack_from("<HHIQI", page, 0)
        if magic == 0:
            break
        hdr = 40 if info & 0x0002 else 24  # XLP_LONG_HEADER on the first page of a segment
        if n == 0 and info & 0x0001:  # tail of a record that began in the previous segment
            skip = align(rem_len, 8)
        s.page_starts.append(len(s.data))
        s.file_offsets.append(n * PAGE + hdr)
        s.data += page[hdr:]
    return s, skip


def parse_record(rec: bytes, lsn: int) -> Record:
    _tot, xid, _prev, info, rmid, _crc = struct.unpack_from("<IIQBBxxI", rec, 0)
    off, datatotal = 24, 0
    heads: list[tuple[int, int, bool, int, int]] = []  # rel, blkno, has_image, bimg_len, data_len
    main_len, prev_rel = 0, 0
    while len(rec) - off > datatotal:
        bid = rec[off]
        off += 1
        if bid == 255:
            main_len = rec[off]
            off += 1
            datatotal += main_len
        elif bid == 254:
            (main_len,) = struct.unpack_from("<I", rec, off)
            off += 4
            datatotal += main_len
        elif bid == 253:  # replication origin
            off += 2
        elif bid == 252:  # top-level xid
            off += 4
        else:
            fork_flags, data_len = struct.unpack_from("<BH", rec, off)
            off += 3
            bimg_len = 0
            if fork_flags & 0x10:  # BKPBLOCK_HAS_IMAGE
                bimg_len, _hole_off, bimg_info = struct.unpack_from("<HHB", rec, off)
                off += 5
                if bimg_info & 0x1C and bimg_info & 0x01:  # compressed and has hole
                    off += 2
            if not fork_flags & 0x80:  # BKPBLOCK_SAME_REL
                _spc, _db, prev_rel = struct.unpack_from("<III", rec, off)
                off += 12
            (blkno,) = struct.unpack_from("<I", rec, off)
            off += 4
            heads.append((prev_rel, blkno, bool(fork_flags & 0x10), bimg_len, data_len if fork_flags & 0x20 else 0))
            datatotal += bimg_len + (data_len if fork_flags & 0x20 else 0)
    blocks: list[Block] = []
    for rel, blkno, has_image, bimg_len, data_len in heads:
        off += bimg_len
        blocks.append(Block(rel, blkno, has_image, rec[off : off + data_len]))
        off += data_len
    return Record(lsn, xid, rmid, info, blocks, rec[off : off + main_len])


def iter_records(raw: bytes, seg_start_lsn: int):
    s, pos = read_segment(raw, seg_start_lsn)
    while pos + 24 <= len(s.data):
        (tot,) = struct.unpack_from("<I", s.data, pos)
        if tot == 0:
            return
        yield parse_record(bytes(s.data[pos : pos + tot]), s.lsn(pos))
        pos = align(pos + tot, 8)


# --------------------------------------------------------------------------- tuples + catalog


@dataclass(frozen=True)
class Col:
    name: str
    typid: int
    attlen: int
    align: str
    dropped: bool


@dataclass
class RawTuple:
    natts: int
    infomask: int
    hoff: int
    payload: bytes  # everything after the 23-byte fixed header: null bitmap, padding, column data


def parse_tuple(buf: bytes) -> RawTuple:
    infomask2, infomask, hoff = struct.unpack_from("<HHB", buf, 0)
    return RawTuple(infomask2 & 0x07FF, infomask, hoff, buf[5:])


def decode_value(typid: int, raw: bytes) -> object:
    match typid:
        case 23:
            return struct.unpack("<i", raw)[0]
        case 20:
            return struct.unpack("<q", raw)[0]
        case 21:
            return struct.unpack("<h", raw)[0]
        case 16:
            return raw != b"\x00"
        case 25 | 1043:
            return raw.decode()
        case _:
            return f"0x{raw.hex()}"


def decode_tuple(t: RawTuple, cols: list[Col]) -> dict[str, object]:
    bitmap = t.payload[: (t.natts + 7) // 8] if t.infomask & 0x0001 else b""
    data = t.payload[t.hoff - HEAP_HDR :]
    out: dict[str, object] = {}
    off = 0
    for i in range(t.natts):
        col = cols[i]
        if bitmap and not (bitmap[i // 8] >> (i % 8)) & 1:
            out[col.name] = None
            continue
        if col.attlen > 0:
            off = align(off, ALIGN[col.align])
            raw = data[off : off + col.attlen]
            if len(raw) < col.attlen:
                raise ValueError(f"ran off the end reading {col.name} ({col.attlen} bytes @ {off})")
            off += col.attlen
        else:  # varlena
            if data[off] == 0:
                off = align(off, ALIGN[col.align])
            if data[off] & 1:  # 1-byte header
                n = data[off] >> 1
                raw = data[off + 1 : off + n]
            else:
                n = struct.unpack_from("<I", data, off)[0] >> 2
                raw = data[off + 4 : off + n]
            off += n
        if not col.dropped:
            out[col.name] = decode_value(col.typid, raw)
    for col in cols[t.natts :]:  # columns added after this tuple was written
        if not col.dropped:
            out[col.name] = None
    return out


# --------------------------------------------------------------------------- heap record -> change


@dataclass
class Change:
    op: str
    lsn: int
    xid: int
    rel: int
    blkno: int
    offnum: int
    new: RawTuple | None = None
    old: RawTuple | None = None
    partial: bool = False
    flags: int = 0


def heap_change(r: Record) -> Change | None:
    op = r.info & 0x70
    blk = r.blocks[0] if r.blocks else None
    if blk and not blk.data and op != HEAP_DELETE:  # full-page image replaced the tuple (catalog tables here)
        return None
    if op == HEAP_INSERT and blk:
        offnum, flags = struct.unpack_from("<HB", r.main, 0)
        return Change("INSERT", r.lsn, r.xid, blk.rel, blk.blkno, offnum, new=parse_tuple(blk.data), flags=flags)
    if op in (HEAP_UPDATE, HEAP_HOT_UPDATE) and blk:
        _xmax, old_off, _ib, flags, _nx, new_off = struct.unpack_from("<IHBBIH", r.main, 0)
        old = parse_tuple(r.main[14:]) if flags & 0x0C and len(r.main) > 14 else None
        partial = bool(flags & 0x60)  # prefix/suffix of the new tuple elided; needs the old page to rebuild
        new = None if partial else parse_tuple(blk.data)
        return Change(
            "UPDATE", r.lsn, r.xid, blk.rel, blk.blkno, new_off, new=new, old=old, partial=partial, flags=flags
        )
    if op == HEAP_DELETE and blk:
        _xmax, offnum, _ib, flags = struct.unpack_from("<IHBB", r.main, 0)
        old = parse_tuple(r.main[8:]) if flags & 0x06 and len(r.main) > 8 else None
        return Change("DELETE", r.lsn, r.xid, blk.rel, blk.blkno, offnum, old=old, flags=flags)
    return None


# --------------------------------------------------------------------------- demo driver


def fmt_lsn(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def head(title: str) -> None:
    print(f"\n\033[1m== {title} ==\033[0m")


def snapshot(conn: psycopg.Connection) -> tuple[int, list[Col]]:
    """Read the *live* catalog for `demo`. walshadow gets this per-record from its replayed shadow instead."""
    filenode = conn.execute("SELECT pg_relation_filenode('demo')").fetchone()[0]  # ty: ignore[not-subscriptable]
    rows = conn.execute(
        "SELECT attname, atttypid::int, attlen, attalign, attisdropped FROM pg_attribute "
        "WHERE attrelid = 'demo'::regclass AND attnum > 0 ORDER BY attnum"
    ).fetchall()
    return filenode, [Col(n, t, ln, a, d) for n, t, ln, a, d in rows]


def run(conn: psycopg.Connection, sql: LiteralString) -> None:
    print(f"  \033[36m{sql}\033[0m")
    conn.execute(sql)


def fetch_segment(name: str) -> bytes:
    OUT_DIR.mkdir(exist_ok=True)
    path = OUT_DIR / name
    pgdata = psycopg.connect(DSN, autocommit=True).execute("SHOW data_directory").fetchone()[0]  # ty: ignore[not-subscriptable]
    subprocess.run(["docker", "cp", f"{CONTAINER}:{pgdata}/pg_wal/{name}", str(path)], check=True, capture_output=True)
    return path.read_bytes()


def show_row(row: dict[str, object] | None) -> str:
    return "<unknown: record has no key/tuple>" if row is None else str(row)


def main() -> int:
    conn = psycopg.connect(DSN, autocommit=True)
    wal_level = conn.execute("SHOW wal_level").fetchone()[0]  # ty: ignore[not-subscriptable]
    print(f"wal_level = {wal_level}   (try `just demo-replica` to see what changes)")
    conn.execute("DROP TABLE IF EXISTS demo")
    conn.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'wal101'")

    head("0. Workload")
    run(conn, "CREATE TABLE demo (id int PRIMARY KEY, name text, qty int)")
    history: dict[int, list[Col]] = {}
    fnode, cols = snapshot(conn)
    history[fnode] = cols
    first_filenode = fnode
    have_slot = wal_level == "logical"
    if have_slot:
        conn.execute("SELECT pg_create_logical_replication_slot('wal101', 'test_decoding')")
    conn.execute("SELECT pg_switch_wal()")  # start the workload at a fresh segment
    start_lsn = conn.execute("SELECT pg_current_wal_lsn()").fetchone()[0]  # ty: ignore[not-subscriptable]
    seg_name = conn.execute("SELECT pg_walfile_name(%s::pg_lsn)", (start_lsn,)).fetchone()[0]  # ty: ignore[not-subscriptable]

    run(conn, "INSERT INTO demo VALUES (1, 'alice', 10)")
    run(conn, "INSERT INTO demo VALUES (2, 'bob', 20)")
    run(conn, "UPDATE demo SET qty = 15 WHERE id = 1")
    print("  \033[36mBEGIN; INSERT INTO demo VALUES (99, 'ghost', 0); ROLLBACK;\033[0m")
    conn.execute("BEGIN")
    conn.execute("INSERT INTO demo VALUES (99, 'ghost', 0)")
    conn.execute("ROLLBACK")
    run(conn, "DELETE FROM demo WHERE id = 2")
    run(conn, "ALTER TABLE demo ALTER COLUMN qty TYPE bigint   -- table REWRITE: new relfilenode, wider column")
    fnode2, cols2 = snapshot(conn)
    history[fnode2] = cols2
    run(conn, "INSERT INTO demo VALUES (3, 'carol', 3000000000)   -- does not fit in int4")
    run(conn, "ALTER TABLE demo ADD COLUMN note text")
    fnode3, cols3 = snapshot(conn)
    history[fnode3] = cols3  # same file as fnode2, one more column
    run(conn, "INSERT INTO demo VALUES (4, 'dave', 1, 'hi')")
    conn.execute("SELECT pg_switch_wal()")

    head("1. Logical decoding  (done INSIDE the source, per slot)")
    if have_slot:
        for xid, data in conn.execute("SELECT xid, data FROM pg_logical_slot_get_changes('wal101', NULL, NULL)"):
            if "BEGIN" in data or "COMMIT" in data:
                continue
            print(f"  xid {xid}: {data}")
        print(
            "  -> readable, committed-only, aborted 'ghost' never shows up. But the source paid CPU + a slot holds WAL."
        )
        conn.execute("SELECT pg_drop_replication_slot('wal101')")
    else:
        print("  unavailable: wal_level=replica cannot create a logical slot")

    raw = fetch_segment(seg_name)
    start_of_seg = int(start_lsn.split("/")[0], 16) << 32 | (int(start_lsn.split("/")[1], 16) & ~(SEG - 1))
    records = list(iter_records(raw, start_of_seg))
    known = set(history)
    heap = [(r, c) for r in records if r.rmid == RM_HEAP and (c := heap_change(r)) and c.rel in known]

    head(f"2. Raw physical WAL  (what a replica gets; segment {seg_name})")
    print("  This is ALL the WAL tells you about the first INSERT - no column names, no types:")
    r0, c0 = heap[0]
    assert c0.new is not None
    print(
        f"    lsn={fmt_lsn(r0.lsn)} xid={r0.xid} rmgr=Heap op=INSERT "
        f"relfilenode={c0.rel} block={c0.blkno} offset={c0.offnum}"
    )
    print(f"    natts={c0.new.natts} tuple bytes = {c0.new.payload.hex(' ')}")
    print("  Which of those bytes is the int, which the text? Only the catalog knows -> hence the 'shadow'.")

    head("3. Catalog-aware decode  (what walshadow does, outside the source)")
    pending: dict[int, list[Change]] = {}
    changes_by_lsn = {c.lsn: c for _, c in heap}
    for r in records:
        if r.lsn in changes_by_lsn:
            pending.setdefault(r.xid, []).append(changes_by_lsn[r.lsn])
        elif r.rmid == RM_XACT and (r.info & 0x70) == XACT_COMMIT and r.xid in pending:
            for c in pending.pop(r.xid):
                cols_for = history[c.rel]
                new: dict[str, object] | None
                old: dict[str, object] | None
                try:
                    new = decode_tuple(c.new, cols_for) if c.new else None
                    old = decode_tuple(c.old, cols_for) if c.old else None
                except (ValueError, IndexError, struct.error) as e:
                    new, old = dict[str, object]({"<decode error>": str(e)}), None
                what = show_row(new if c.op != "DELETE" else old)
                if c.partial:
                    what = "<partial new tuple: wal_level=replica elides unchanged prefix/suffix>"
                tail = f"   old_key={old}" if c.op == "UPDATE" and old else ""
                print(
                    f"  COMMIT xid {r.xid} @ {fmt_lsn(r.lsn)}  {c.op:6} "
                    f"(file {c.rel}, blk {c.blkno}, off {c.offnum})  {what}{tail}"
                )
        elif r.rmid == RM_XACT and (r.info & 0x70) == XACT_ABORT and r.xid in pending:
            dropped = pending.pop(r.xid)
            print(
                f"  ABORT  xid {r.xid}: dropped {len(dropped)} change(s) that ARE in physical WAL (rolled-back rows!)"
            )
    print("  Note the ALTER ... TYPE transaction: the rewrite RE-INSERTS every live row into a new file.")
    print("  A naive consumer would emit alice again as a fresh INSERT - walshadow has 'rewrite barriers' for this.")
    print("  Also: DELETE only carries the key (other columns NULL) - that is the replica identity at work.")

    head("4. Same bytes, but decoded with only the CURRENT catalog  (a static schema snapshot)")
    latest = history[fnode3]
    ok = bad = 0
    for _, c in heap:
        if c.rel != first_filenode or not c.new:
            continue
        try:
            row = decode_tuple(c.new, latest)
            print(f"  {c.op:6} -> {row}   <- looks fine?")
            ok += 1
        except (ValueError, IndexError, struct.error) as e:
            print(f"  {c.op:6} -> \033[31mDECODE ERROR\033[0m: {e}")
            bad += 1
    print(f"  {bad} of {ok + bad} pre-ALTER records fail or lie: qty was int4 when written, int8 now.")
    print(
        "  This is why walshadow replays catalog WAL into a schema-only shadow Postgres and keeps descriptor history."
    )

    head("5. wal_level matters")
    kinds = {c.op: c for _, c in heap}
    d = kinds.get("DELETE")
    u = kinds.get("UPDATE")
    if d:
        print(
            f"  DELETE carries the old key? {'yes' if d.old else 'NO - only (block, offset); you must read the page'}"
        )
    if u:
        print(f"  UPDATE new tuple complete? {'NO - prefix/suffix elided' if u.partial else 'yes'}")
    print(
        "  Physical WAL at wal_level=replica is for *replicas*; 'logical' adds keys and full tuples.\n"
        "  That is why walshadow still needs wal_level=logical."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
