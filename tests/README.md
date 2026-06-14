# Onedis Test Layout

`command_semantics/`

In-process integration tests for command parsing, dispatch, and storage semantics.
These tests construct `Db`, `KvStore`, `Command`, or server components directly.
They do not start a real `onedis-server` TCP listener.
Name files by the behavior they cover. For example, full-text tests are split by
domains such as lifecycle, schema creation, query parsing, aggregation, vector
search, and cluster contract; do not name files after implementation phases.

Run with:

```sh
cargo test -p onedis-server --tests
```

`tcp_e2e/`

Real end-to-end tests. These tests spawn `onedis-server` and drive commands
through a Redis TCP client connection.

Run with:

```sh
cargo test -p onedis-server --features tcp-integration-tests --test basic_commands_tcp_e2e --test connect_commands_tcp_e2e --test hash_commands_tcp_e2e --test info_command_tcp_e2e --test keys_performance_tcp_e2e --test move_command_tcp_e2e --test scan_command_tcp_e2e --test sdiff_command_tcp_e2e --test sscan_command_tcp_e2e --test transactions_tcp_e2e
```

Category-local support files

No shared support code should live at the test root. Put helpers beside the test
category that owns them, so Cargo does not treat helper files as standalone
integration test targets.
