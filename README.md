<div align="center">

# walshadow-101

## Is ClickHouse's physical-WAL Postgres CDC revolutionary, or just a very good press release?

[![CI](https://github.com/glukicov/walshadow-101/actions/workflows/ci.yml/badge.svg)](https://github.com/glukicov/walshadow-101/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.13%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

*An unofficial, opinionated review of [ClickHouse/walshadow](https://github.com/ClickHouse/walshadow), plus a small demo that decodes Postgres WAL by hand. Not affiliated with ClickHouse.*

</div>

## TL;DR

- 🧠 **The idea is real.** Tail *physical* WAL, decode it off the source, and keep a schema-only "shadow" Postgres around to remember what the columns looked like at each LSN.
- 📣 **The headline is rounded up.** "50x faster than PeerDB" is a vendor benchmark against their own product. "No load on Postgres" still needs `wal_level=logical`.
- 🚧 **It's v0.1.2.** Docs list real gaps (no prepared transactions, TOAST edge cases, same-major-version shadow).
- 🔌 **BigQuery/lakehouse?** The architecture transfers; the code doesn't (no sink abstraction, ClickHouse-shaped throughout).
- 🧪 **You can poke it** with the demos below. No Postgres install needed, just Docker and [uv](https://docs.astral.sh/uv/).

## Claims vs. what I saw

| Claim | Verdict |
|---|---|
| Shadow catalog tracks DDL, decodes in parallel off-box | Real, and the best idea here |
| ~200 ms commit→visible | I got **~0.8–1.1 s** on a laptop (Docker Desktop, 1 row, includes polling overhead). Sanity check, not a benchmark |
| "No logical replication overhead" | Half true: no slots, but `wal_level=logical` is still required (`just demo-replica` shows why) |
| Works with your cloud Postgres | Not RDS/Aurora/Cloud SQL/Supabase: they don't expose physical WAL |

Full write-up, alternatives table and BigQuery/lakehouse analysis: **[docs/WRITEUP.md](docs/WRITEUP.md)**.

## Quickstart

```bash
just demo           # hand-rolled WAL decoder, wal_level=logical
just demo-replica   # same, wal_level=replica: DELETE loses its key, UPDATE loses its tuple
```

What `just demo` prints, in order: the workload, logical decoding output, the raw physical WAL bytes for one `INSERT` (no column names, no types), catalog-aware decode (aborted rows dropped), and the same bytes decoded with a *stale* catalog, which falls over after an `int → bigint` rewrite. That last one is the whole reason for the shadow.

The real thing (builds a Rust image, several minutes the first time):

```bash
just ws-clone && just ws-build && just ws-up && just ws-update
just ws-down
```

## Layout

```
demo/wal101.py             WAL segment parser + heap tuple decoder (PG17 layout)
tests/                     unit tests for the decoder
docker/macos-override.yml  fixes upstream quickstart's init script on Docker Desktop for Mac
docs/WRITEUP.md            the long version
justfile                   all the commands
```

## Caveats

- The demo is a teaching toy: PG17 only, a handful of types, no TOAST, no full-page-image extraction.
- I did not reproduce ClickHouse's benchmark. My latency number is one laptop and five tries.
- Opinions are mine, from the repo docs, the launch post and a few independent critiques (linked in the write-up). Corrections welcome via issues.
- walshadow itself is AGPL-3.0 and is cloned at runtime into a gitignored directory, never redistributed here.
