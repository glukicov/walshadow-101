# walshadow: hype vs. substance

*ClickHouse/walshadow v0.1.2 (AGPL-3.0, ~96k lines of Rust, started May 2026). I read the code and docs, built and ran it, wrote a mini version of the idea (`just demo`), and read the launch post, HN thread and two independent critiques. I did **not** reproduce their benchmark.*

## Verdict

Clever engineering, real but narrow win, marketing rounded up. The core trick is new to me. "50x" and "no load on Postgres" I would not repeat in a design review.

## What it does

Logical decoding makes the *source* turn WAL into rows. walshadow instead tails **physical** WAL like a replica and decodes it *off-box*. The catch: a physical tuple is bytes plus a relfilenode, undecodable without the schema *as of that record* (the demo shows a `qty int→bigint` rewrite breaking a naive decoder). So it runs a **schema-only shadow Postgres** that replays just catalog WAL, keeps versioned descriptors, and decodes user-table records with parallel Rust workers straight into ClickHouse Native blocks. No slots; `ADD COLUMN DEFAULT`, `RENAME`, `DROP COLUMN` work.

## Real vs. hype

| Claim | Verdict |
|---|---|
| Shadow catalog: let Postgres track its own DDL, decode in parallel elsewhere | **Real, and the best idea here.** |
| ~200 ms vs ~10 s ("50x") | **Plausible, apples-to-oranges.** Vendor benchmark, one table, vs their own PeerDB, whose config I couldn't find (logical tools' latency is mostly tunable batching). My laptop run: commit→visible **~0.8–1.1 s** (Docker Desktop, 1 row, incl. ~150 ms polling overhead). Sub-second, not 200 ms. Not a benchmark, just a sanity check. |
| 289k rows/s | Equals the insert rate of the source; they say it "will vary by workload". |
| "No slots, no load on Postgres" | **Half true.** No slot-pinned WAL, no decode CPU. But `wal_level=logical` is still required (`just demo-replica` shows why: at `replica`, DELETE has no key and UPDATE no full tuple), plus a replication connection and a base backup or `COPY` for initial load. |
| "Zero operational overhead" | **No.** You now run a daemon + a shadow Postgres whose **major version must match the source** + ClickHouse. Docs: "not an HA system", unplanned promotion unsupported. |
| Production-ready | **0.1.2.** Documented gaps: no prepared transactions, one transaction's cross-table writes can become visible at different moments, reused TOAST IDs can resolve to the wrong generation, "unsupported behavior is not uniformly rejected at startup". [The Build](https://thebuild.com/blog/the-shadow-knows/) calls the WAL byte format something Postgres "never promised you anything about". |

**Commercial subtext:** RDS, Aurora, Cloud SQL and Supabase don't expose physical WAL (a ClickHouse engineer said so on HN). It works on self-managed Postgres and on ClickHouse Managed Postgres. Great feature for a managed product, hard sell as bring-your-own-cloud-DB tooling.

## Transformative?

For self-hosted Postgres → ClickHouse teams who hate slots: a real step. For everyone else: a good proof that physical-WAL CDC is feasible, not a reason to migrate a working Debezium/PeerDB/Datastream pipeline.

## Alternatives

**PeerDB/ClickPipes** (same target, logical, works on RDS: the boring correct choice) · **Debezium + Kafka** (mature, heavy) · **Estuary, Airbyte, Fivetran** (managed logical CDC) · **Datastream** (→ BigQuery CDC upserts) · **RisingWave, OLake** (→ Iceberg) · **logical decoding on a PG16+ standby**: the underrated 80%, moves decode cost and slots off the primary and keeps the supported API.

## Reusable for BigQuery / lakehouse CDC?

**The idea, yes. The code, mostly no.** There is no sink abstraction: output is ClickHouse Native blocks (`src/emit/ch_emitter.rs`) and correctness leans on ReplacingMergeTree (`_lsn`, `FINAL`). By size, filter/catalog/decode/xact/source ≈ 35k lines are destination-agnostic in principle; emit + backfill ≈ 32k are not, and initial load without gaps is the hard half. `_lsn` ordering maps neatly onto BigQuery's `_CHANGE_SEQUENCE_NUMBER` or Iceberg equality deletes, so a port is conceivable, i.e. a rewrite.

More to the point, **Postgres isn't your latency floor there**: Storage Write API streams and Iceberg commits (seconds, small files, compaction) dominate long before 200 ms vs 10 s matters. What transfers is the architecture lesson: decode off the source, in parallel, with a versioned schema.

## Use it if all true

Self-managed Postgres (or ClickHouse Managed PG) · ClickHouse target · you own PG upgrades · sub-second matters · 0.x software is fine. Otherwise watch the repo.

## Try it

`just demo` · `just demo-replica` · `just ws-clone && just ws-build && just ws-up && just ws-update` (real thing; `docker/macos-override.yml` works around upstream's init script failing on Docker Desktop for Mac) · `just ws-down`

[Launch post](https://clickhouse.com/blog/introducing-walshadow) · [product page](https://clickhouse.com/cloud/postgres/walshadow) · [HN](https://news.ycombinator.com/item?id=49660791) · [The Build](https://thebuild.com/blog/the-shadow-knows/) · [Pisda critique](https://kdpisda.in/walshadow-postgres-wal-level-logical-still-required/) · [BigQuery CDC](https://docs.cloud.google.com/bigquery/docs/change-data-capture) · repo: `docs/limitations.md`, `architecture/README.md`
