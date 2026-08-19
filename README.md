# OneDis

OneDis is a Redis-compatible data service built on kv-engine. It implements common Redis data
commands plus JSON, full-text search, and native vector search, while kv-engine owns durability,
object-storage integration, and storage-role topology.

OneDis intentionally does not implement Redis replication, Sentinel, Cluster, RDB, or AOF. Such
commands return an explicit unsupported error instead of reporting fake success. The authoritative
command scope is [docs/redis_user_command_compat.json](docs/redis_user_command_compat.json).

## Build and run

Requirements: Rust 1.97.1 plus the pinned kv-engine and sibling Tantivy checkouts. Bootstrap them
from a clean clone with `scripts/bootstrap-dependencies.sh`.

```bash
cargo build --locked --release
./target/release/onedis-server --config config/onedis.toml
```

The server exposes RESP2 and RESP3 on the configured Redis port. Prometheus metrics, liveness, and
readiness are available on port `9121` by default:

```bash
redis-cli -p 6379 HELLO 3
curl --fail http://127.0.0.1:9121/healthz
curl --fail http://127.0.0.1:9121/readyz
curl --fail http://127.0.0.1:9121/metrics
```

Invalid configuration, an unavailable listen address, an unusable data directory, or kv-engine
initialization failure causes a non-zero startup exit. SIGTERM stops acceptance, removes readiness,
drains active work up to the configured deadline, stops index maintenance, and checkpoints the WAL.

## Resource limits

Production limits are configured with `ONEDIS_LIMIT_*` environment variables. Defaults and full
descriptions are in [docs/operations/resource-limits.md](docs/operations/resource-limits.md).
Invalid or zero values fail startup.

## Deployment and operations

- [Container image](docker/README.md)
- [Production runbook](docs/operations/production-runbook.md)
- [Upgrade and rollback](docs/operations/upgrade-rollback.md)
- [Release and supply-chain gate](docs/operations/release.md)
- [Security policy](SECURITY.md)
- [Benchmark system](benchmarks/README.md)
- [Command compatibility](docs/redis_user_command_compat.json)

Run all correctness targets with `cargo test --all-targets --no-fail-fast`. Run the exhaustive
benchmark contract in short mode with:

```bash
BENCHMARK_SUITE_PROFILE=full BENCHMARK_QUICK_CASE_COVERAGE=1 benchmarks/run_all.sh
```

Publishable benchmark results require a dedicated host and the full profile described in the
benchmark documentation.
