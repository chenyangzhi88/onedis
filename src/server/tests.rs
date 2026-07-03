    use crate::args::ResolvedArgs;
    use crate::command::Command;
    use crate::command_executor::CommandExecutor;
    use crate::frame::Frame;
    use crate::network::session_manager::SessionManager;
    use crate::store::db::{Db, StreamId, Structure};
    use crate::store::db_manager::DatabaseManager;
    use crate::store::kv_store::KvStore;
    use crate::store::ttl::{TtlConfig, TtlManager, VersionCounter};
    use crate::wasm::WasmRegistry;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::{
        Handler, Server, append_array_len, append_bulk_string, append_error, append_integer,
        append_null, append_simple_string, append_usize_decimal, borrowed_list_push_supported,
        borrowed_lrange_supported, borrowed_plain_set_supported, borrowed_read_supported,
        find_crlf, format_command_for_monitor, parse_borrowed_plain_hset_commands,
        parse_borrowed_plain_set_commands, parse_borrowed_resp_commands, parse_i64_ascii,
        parse_usize_ascii,
    };

    fn test_db() -> Db {
        let unique = format!(
            "onedis-server-transaction-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = test_root(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        Db::new(0, store, version_counter, ttl_manager)
    }

    fn command(args: &[&str]) -> Command {
        let frame = Frame::Array(
            args.iter()
                .map(|arg| Frame::bulk_string((*arg).to_string()))
                .collect(),
        );
        Command::parse_from_frame(frame).unwrap()
    }

    fn test_root(unique: String) -> std::path::PathBuf {
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target/onedis-test-data"));
        target_dir.join(unique)
    }

    fn test_args(databases: usize, requirepass: Option<&str>) -> Arc<ResolvedArgs> {
        let unique = format!(
            "onedis-server-handler-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = test_root(unique);
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        let config = root.join("onedis.toml");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &config,
            format!(
                r#"
[db]
path = "{}"

[wal]
dir = "{}"

[onedis_server]
port = 0
bind = "127.0.0.1"
databases = {}
hz = 10.0
loglevel = "info"
maxclients = 0
"#,
                db_path.display(),
                wal_dir.display(),
                databases
            ),
        )
        .unwrap();
        Arc::new(ResolvedArgs {
            config: config.to_string_lossy().into_owned(),
            requirepass: requirepass.map(ToString::to_string),
            bind: "127.0.0.1".to_string(),
            databases,
            hz: 10.0,
            port: 0,
            loglevel: "info".to_string(),
            maxclients: 0,
            observability_enabled: false,
            metrics_bind: "127.0.0.1".to_string(),
            metrics_port: 0,
            slow_command_threshold_ms: 10,
        })
    }

    async fn connected_streams() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = listener.accept();
        let connect = TcpStream::connect(addr);
        let (accepted, connected) = tokio::join!(accept, connect);
        let (server_stream, _) = accepted.unwrap();
        (server_stream, connected.unwrap())
    }

    async fn test_handler(databases: usize, requirepass: Option<&str>) -> (Handler, TcpStream) {
        let args = test_args(databases, requirepass);
        let db_manager = Arc::new(DatabaseManager::new_async(args.clone()).await);
        let session_manager = Arc::new(SessionManager::new());
        let command_executor = Arc::new(CommandExecutor::new(2, 16).unwrap());
        let wasm_registry = Arc::new(WasmRegistry::new());
        let (server_stream, client_stream) = connected_streams().await;
        (
            Handler::new(
                db_manager,
                session_manager,
                command_executor,
                wasm_registry,
                server_stream,
                args,
            ),
            client_stream,
        )
    }

    fn text(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(bytes)
    }

    #[test]
    fn server_new_initializes_core_runtime_components_without_binding_socket() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let args = test_args(3, Some("secret"));
        let server = rt.block_on(Server::new(args.clone()));

        assert_eq!(server.args.databases, 3);
        assert_eq!(server.args.requirepass.as_deref(), Some("secret"));
        assert_eq!(server.session_manager.get_connection_count(), 0);
        assert!(Arc::strong_count(&server.db_manager) >= 1);
        assert!(Arc::strong_count(&server.command_executor) >= 1);
        assert!(Arc::strong_count(&server.wasm_registry) >= 1);
    }

    #[test]
    fn transaction_execution_error_aborts_without_partial_writes() {
        let db = test_db();
        db.insert(
            "bad-number".to_string(),
            Structure::String("not-a-number".to_string()),
        );
        let txn_db = db.transactional_view().unwrap();

        let result = Handler::execute_transaction_commands(
            &txn_db,
            vec![
                command(&["set", "side-effect", "written"]),
                command(&["incr", "bad-number"]),
            ],
            16,
        )
        .unwrap();

        assert!(matches!(result, Frame::Error(message) if message.contains("EXECABORT")));
        assert!(db.get("side-effect").is_none());
        assert!(matches!(
            db.get("bad-number"),
            Some(Structure::String(value)) if value == "not-a-number"
        ));
    }

    #[test]
    fn transaction_success_commits_all_results() {
        let db = test_db();
        let txn_db = db.transactional_view().unwrap();

        let result = Handler::execute_transaction_commands(
            &txn_db,
            vec![
                command(&["set", "txn-key", "value"]),
                command(&["get", "txn-key"]),
            ],
            16,
        )
        .unwrap();

        assert!(matches!(result, Frame::Array(values) if values.len() == 2));
        assert!(matches!(
            db.get("txn-key"),
            Some(Structure::String(value)) if value == "value"
        ));
    }

    #[test]
    fn server_command_classifiers_cover_blocking_mutating_worker_and_direct_paths() {
        let blpop = command(&["blpop", "list", "0.5"]);
        assert!(Handler::is_blocking_list_command(&blpop));
        assert_eq!(Handler::blocking_list_timeout_secs(&blpop), 0.5);
        assert!(Handler::is_list_mutating_command(&blpop));

        let brpop = command(&["brpop", "list", "1"]);
        assert!(Handler::is_blocking_list_command(&brpop));
        assert_eq!(Handler::blocking_list_timeout_secs(&brpop), 1.0);

        let brpoplpush = command(&["brpoplpush", "src", "dst", "2"]);
        assert!(Handler::is_blocking_list_command(&brpoplpush));
        assert_eq!(Handler::blocking_list_timeout_secs(&brpoplpush), 2.0);

        let blmove = command(&["blmove", "src", "dst", "left", "right", "0.25"]);
        assert!(Handler::is_blocking_list_command(&blmove));
        assert_eq!(Handler::blocking_list_timeout_secs(&blmove), 0.25);

        let blmpop = command(&["blmpop", "1.5", "1", "list", "left", "count", "2"]);
        assert!(Handler::is_blocking_list_command(&blmpop));
        assert_eq!(Handler::blocking_list_timeout_secs(&blmpop), 1.5);

        let bzpopmin = command(&["bzpopmin", "z", "0.75"]);
        assert!(Handler::is_blocking_zset_command(&bzpopmin));
        assert!(Handler::is_zset_mutating_command(&bzpopmin));
        assert_eq!(Handler::blocking_zset_timeout_secs(&bzpopmin), 0.75);

        let bzpopmax = command(&["bzpopmax", "z", "1.25"]);
        assert!(Handler::is_blocking_zset_command(&bzpopmax));
        assert_eq!(Handler::blocking_zset_timeout_secs(&bzpopmax), 1.25);

        let bzmpop = command(&["bzmpop", "2.5", "1", "z", "min", "count", "2"]);
        assert!(Handler::is_blocking_zset_command(&bzmpop));
        assert_eq!(Handler::blocking_zset_timeout_secs(&bzmpop), 2.5);

        let xread = command(&["xread", "block", "5", "streams", "s", "0-0"]);
        assert!(Handler::is_blocking_stream_command(&xread));
        assert!(Handler::is_stream_mutating_command(&command(&[
            "xadd", "s", "*", "f", "v"
        ])));
        assert_eq!(Handler::blocking_stream_timeout_ms(&xread), Some(5));
        assert!(!Handler::is_blocking_stream_command(&command(&[
            "xread", "streams", "s", "0-0"
        ])));

        let xreadgroup = command(&[
            "xreadgroup",
            "group",
            "g",
            "c",
            "block",
            "6",
            "streams",
            "s",
            ">",
        ]);
        assert!(Handler::is_blocking_stream_command(&xreadgroup));
        assert_eq!(Handler::blocking_stream_timeout_ms(&xreadgroup), Some(6));

        assert!(Handler::can_apply_direct(&command(&["get", "k"])));
        assert!(Handler::can_apply_direct(&command(&["set", "k", "v"])));
        assert!(Handler::can_apply_on_worker(&command(&[
            "zrange", "z", "0", "-1"
        ])));
        assert!(!Handler::can_apply_direct(&command(&["auth", "pw"])));
        assert!(!Handler::can_apply_on_worker(&command(&["auth", "pw"])));
        assert!(!Handler::can_apply_direct(&command(&["multi"])));
        assert!(!Handler::can_apply_direct(&command(&["unknown-command"])));

        assert!(!Handler::is_list_mutating_command(&command(&["get", "k"])));
        assert!(Handler::is_list_mutating_command(&command(&[
            "lpush", "l", "v"
        ])));
        assert!(Handler::is_zset_mutating_command(&command(&[
            "zadd", "z", "1", "m"
        ])));
        assert!(!Handler::is_zset_mutating_command(&command(&[
            "zcard", "z"
        ])));
    }

    #[test]
    fn borrowed_parser_helpers_and_resp_appenders_cover_success_and_error_edges() {
        let resp = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\nc\r\n";
        let parsed = parse_borrowed_resp_commands(resp).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], vec![b"GET".as_slice(), b"a".as_slice()]);
        assert!(parse_borrowed_resp_commands(b"*1\r\n$4\r\nPING").is_none());
        assert!(parse_borrowed_resp_commands(b"+OK\r\n").is_none());

        let set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        assert_eq!(
            parse_borrowed_plain_set_commands(set).unwrap(),
            vec![(b"k".as_slice(), b"v".as_slice())]
        );
        assert!(parse_borrowed_plain_set_commands(resp).is_none());

        let hset = b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n";
        assert_eq!(
            parse_borrowed_plain_hset_commands(hset).unwrap(),
            vec![(b"h".as_slice(), b"f".as_slice(), b"v".as_slice())]
        );
        assert!(parse_borrowed_plain_hset_commands(set).is_none());

        assert!(borrowed_read_supported(&[
            b"GET".as_slice(),
            b"k".as_slice()
        ]));
        assert!(borrowed_read_supported(&[
            b"MGET".as_slice(),
            b"k".as_slice()
        ]));
        assert!(borrowed_read_supported(&[
            b"TYPE".as_slice(),
            b"k".as_slice()
        ]));
        assert!(!borrowed_read_supported(&[]));
        assert!(borrowed_plain_set_supported(&[
            b"set".as_slice(),
            b"k".as_slice(),
            b"v".as_slice(),
        ]));
        assert!(borrowed_list_push_supported(&[
            b"rpush".as_slice(),
            b"l".as_slice(),
            b"v".as_slice(),
        ]));
        assert!(borrowed_lrange_supported(&[
            b"lrange".as_slice(),
            b"l".as_slice(),
            b"0".as_slice(),
            b"-1".as_slice(),
        ]));
        assert!(!borrowed_lrange_supported(&[
            b"lrange".as_slice(),
            b"l".as_slice(),
            b"0".as_slice(),
        ]));

        assert_eq!(find_crlf(b"abc\r\ndef", 0), Some(3));
        assert_eq!(find_crlf(b"abcdef", 0), None);
        assert_eq!(parse_usize_ascii(b"123"), Some(123));
        assert_eq!(parse_usize_ascii(b"12x"), None);
        assert_eq!(parse_i64_ascii(b"-42"), Some(-42));
        assert_eq!(parse_i64_ascii(b"-"), None);
        assert_eq!(parse_i64_ascii(b"9x"), None);

        let mut out = Vec::new();
        append_simple_string(&mut out, "OK");
        append_error(&mut out, "ERR bad");
        append_integer(&mut out, -7);
        append_array_len(&mut out, 2);
        append_bulk_string(&mut out, b"hi");
        append_null(&mut out);
        append_usize_decimal(&mut out, 12345);
        assert_eq!(
            out,
            b"+OK\r\n-ERR bad\r\n:-7\r\n*2\r\n$2\r\nhi\r\n$-1\r\n12345"
        );
    }

    #[test]
    fn handler_fast_paths_acl_pubsub_and_monitor_cover_private_routes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut handler, mut client_stream) = rt.block_on(test_handler(2, None));

        assert_eq!(
            handler
                .try_handle_ping_fast_batch(b"PING\r\nPING\r\n")
                .unwrap(),
            b"+PONG\r\n+PONG\r\n"
        );
        assert_eq!(
            handler
                .try_handle_ping_fast_batch(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nping\r\n")
                .unwrap(),
            b"+PONG\r\n+PONG\r\n"
        );
        assert!(
            handler
                .try_handle_ping_fast_batch(b"PING\r\nSET\r\n")
                .is_none()
        );

        let (auth_handler, _) = rt.block_on(test_handler(1, Some("secret")));
        assert!(
            auth_handler
                .try_handle_ping_fast_batch(b"PING\r\n")
                .is_none()
        );

        let bytes =
            b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        assert_eq!(
            rt.block_on(handler.try_handle_borrowed_fast_batch(bytes))
                .unwrap(),
            b"+OK\r\n+OK\r\n"
        );
        let get_bytes = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n*3\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nx\r\n*2\r\n$6\r\nEXISTS\r\n$1\r\nb\r\n*2\r\n$6\r\nSTRLEN\r\n$1\r\na\r\n*2\r\n$4\r\nTYPE\r\n$1\r\na\r\n";
        let response = rt
            .block_on(handler.try_handle_borrowed_fast_batch(get_bytes))
            .unwrap();
        let text = String::from_utf8_lossy(&response);
        assert!(text.contains("$1\r\n1"));
        assert!(text.contains(":1\r\n"));
        assert!(text.contains("+string\r\n"));

        let hset_bytes = b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n";
        assert_eq!(
            rt.block_on(handler.try_handle_borrowed_fast_batch(hset_bytes))
                .unwrap(),
            b":1\r\n"
        );
        let list_bytes = b"*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\nb\r\n";
        assert_eq!(
            rt.block_on(handler.try_handle_borrowed_fast_batch(list_bytes))
                .unwrap(),
            b":1\r\n:2\r\n"
        );
        let lrange = b"*4\r\n$6\r\nLRANGE\r\n$1\r\nl\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let response = rt
            .block_on(handler.try_handle_borrowed_fast_batch(lrange))
            .unwrap();
        assert!(String::from_utf8_lossy(&response).contains("$1\r\na"));
        assert!(
            rt.block_on(handler.try_handle_borrowed_fast_batch(b"*1\r\n$7\r\nCOMMAND\r\n"))
                .is_none()
        );

        assert!(
            matches!(handler.apply_acl(&["WHOAMI".to_string()]), Frame::BulkString(user) if user == b"default")
        );
        assert!(
            matches!(handler.apply_acl(&["USERS".to_string()]), Frame::Array(values) if !values.is_empty())
        );
        assert!(
            matches!(handler.apply_acl(&["LIST".to_string()]), Frame::Array(values) if !values.is_empty())
        );
        assert!(matches!(
            handler.apply_acl(&[
                "SETUSER".to_string(),
                "limited".to_string(),
                "on".to_string(),
                ">pw".to_string(),
                "+get".to_string(),
            ]),
            Frame::Ok
        ));
        assert!(matches!(
            handler.apply_acl(&["DELUSER".to_string(), "limited".to_string()]),
            Frame::Integer(1)
        ));
        assert!(
            matches!(handler.apply_acl(&["CAT".to_string()]), Frame::Array(values) if values.is_empty())
        );
        assert!(
            matches!(handler.apply_acl(&["HELP".to_string()]), Frame::Array(values) if !values.is_empty())
        );
        assert!(
            matches!(handler.apply_acl(&["BAD".to_string()]), Frame::Error(message) if message.contains("syntax"))
        );

        let subscribe = command(&["subscribe", "news"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&subscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("subscribe"));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["NUMSUB".to_string(), "news".to_string()]),
            Frame::Array(values) if values.len() == 2
        ));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["CHANNELS".to_string()]),
            Frame::Array(values) if values.len() == 1
        ));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["NUMPAT".to_string()]),
            Frame::Integer(0)
        ));

        let psubscribe = command(&["psubscribe", "n*"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&psubscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("psubscribe"));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["NUMPAT".to_string()]),
            Frame::Integer(1)
        ));

        let ssubscribe = command(&["ssubscribe", "shard"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&ssubscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("ssubscribe"));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["SHARDNUMSUB".to_string(), "shard".to_string()]),
            Frame::Array(values) if values.len() == 2
        ));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["SHARDCHANNELS".to_string()]),
            Frame::Array(values) if values.len() == 1
        ));
        assert!(matches!(
            handler.apply_pubsub_introspection(&["UNKNOWN".to_string()]),
            Frame::Array(values) if values.is_empty()
        ));

        let publish = command(&["publish", "news", "payload"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&publish))
            .unwrap()
            .unwrap();
        assert_eq!(bytes, b":2\r\n");
        let mut payload = vec![0; 256];
        let read = rt.block_on(client_stream.read(&mut payload)).unwrap();
        assert!(String::from_utf8_lossy(&payload[..read]).contains("payload"));

        let unsubscribe = command(&["unsubscribe"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&unsubscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("unsubscribe"));

        let unsubscribe_channel = command(&["unsubscribe", "news"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&unsubscribe_channel))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("unsubscribe"));

        let punsubscribe = command(&["punsubscribe", "n*"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&punsubscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("punsubscribe"));

        let sunsubscribe = command(&["sunsubscribe", "shard"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&sunsubscribe))
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("sunsubscribe"));

        let monitor = command(&["monitor"]);
        let bytes = rt
            .block_on(handler.try_apply_pubsub_or_monitor(&monitor))
            .unwrap()
            .unwrap();
        assert_eq!(bytes, b"+OK\r\n");
        assert!(format_command_for_monitor(&command(&["get", "k"])).contains("\"get\""));
    }

    #[test]
    fn borrowed_fast_paths_cover_guards_and_error_branches() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut handler, _) = rt.block_on(test_handler(1, None));

        handler.start_transaction().unwrap();
        assert!(
            rt.block_on(handler.try_handle_borrowed_fast_batch(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
                .is_none()
        );
        handler.clear_transaction();

        let (mut auth_handler, _) = rt.block_on(test_handler(1, Some("secret")));
        assert!(
            rt.block_on(
                auth_handler.try_handle_borrowed_fast_batch(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            )
            .is_none()
        );
        auth_handler.login(None, "secret").unwrap();
        assert!(
            rt.block_on(
                auth_handler
                    .try_handle_borrowed_fast_batch(b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n")
            )
            .is_some()
        );

        let response = handler.handle_borrowed_read_commands(vec![
            vec![b"GET".as_slice()],
            vec![b"EXISTS".as_slice()],
            vec![b"TTL".as_slice(), &[0xff]],
            vec![b"PTTL".as_slice(), b"k".as_slice(), b"extra".as_slice()],
            vec![b"STRLEN".as_slice(), &[0xff]],
            vec![b"TYPE".as_slice(), &[0xff]],
            vec![b"EXISTS".as_slice(), &[0xff], b"missing".as_slice()],
            vec![b"MGET".as_slice(), b"missing".as_slice(), &[0xff]],
        ]);
        let response_text = text(&response);
        assert!(response_text.contains("wrong number of arguments for 'get' command"));
        assert!(response_text.contains("wrong number of arguments for 'exists' command"));
        assert!(response_text.contains("wrong number of arguments for ttl command"));
        assert!(response_text.matches("ERR invalid UTF-8 key").count() >= 3);
        assert!(response_text.contains(":0\r\n"));
        assert!(response_text.contains("*2\r\n$-1\r\n$-1\r\n"));

        let invalid_key = [0xff];
        let response = rt.block_on(handler.handle_borrowed_set_commands(vec![vec![
            b"SET".as_slice(),
            invalid_key.as_slice(),
            b"v".as_slice(),
        ]]));
        assert!(text(&response).contains("ERR invalid UTF-8 key"));

        let response = rt.block_on(handler.try_handle_borrowed_fast_batch(
            b"*4\r\n$4\r\nHSET\r\n$1\r\n\xff\r\n$1\r\nf\r\n$1\r\nv\r\n\
              *4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\n\xff\r\n$1\r\nv\r\n\
              *4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n\xff\r\n",
        ));
        let response = response.unwrap();
        let response_text = text(&response);
        assert!(response_text.contains("ERR invalid UTF-8 key"));
        assert!(response_text.contains("ERR invalid UTF-8 hash field"));
        assert!(response_text.contains("ERR invalid UTF-8 hash value"));

        rt.block_on(
            crate::command_dispatch::handle_command_async(
                handler.get_session().get_db().as_ref(),
                command(&["set", "plain", "value"]),
            ),
        )
        .unwrap();
        let response = rt
            .block_on(handler.try_handle_borrowed_fast_batch(
                b"*3\r\n$5\r\nRPUSH\r\n$5\r\nplain\r\n$1\r\nv\r\n\
                  *3\r\n$5\r\nLPUSH\r\n$5\r\nplain\r\n$1\r\nv\r\n",
            ))
            .unwrap();
        assert_eq!(text(&response).matches("wrong kind of value").count(), 2);

        let response = rt
            .block_on(handler.try_handle_borrowed_fast_batch(
                b"*3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$1\r\na\r\n\
                  *3\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$1\r\nb\r\n\
                  *3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$1\r\nc\r\n",
            ))
            .unwrap();
        assert_eq!(response, b":1\r\n:2\r\n:3\r\n");

        let response = rt
            .block_on(handler.try_handle_borrowed_fast_batch(
                b"*4\r\n$6\r\nLRANGE\r\n$1\r\n\xff\r\n$1\r\n0\r\n$2\r\n-1\r\n\
                  *4\r\n$6\r\nLRANGE\r\n$1\r\nq\r\n$1\r\nx\r\n$2\r\n-1\r\n\
                  *4\r\n$6\r\nLRANGE\r\n$5\r\nplain\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            ))
            .unwrap();
        let response_text = text(&response);
        assert!(response_text.contains("ERR invalid UTF-8 key"));
        assert!(response_text.contains("ERR value is not an integer or out of range"));
        assert!(response_text.contains("wrong kind of value"));
    }

    #[test]
    fn blocking_handlers_pop_ready_values_and_timeout_when_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (handler, _) = rt.block_on(test_handler(1, None));
        let db = handler.get_session().get_db().clone();

        rt.block_on(crate::command_dispatch::handle_command_async(
            &db,
            command(&["rpush", "l", "a", "b"]),
        ))
        .unwrap();
        let bytes = rt
            .block_on(handler.apply_blocking_list_command(command(&["blpop", "l", "0.01"])))
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("$1\r\na"));

        let bytes = rt
            .block_on(handler.apply_blocking_list_command(command(&[
                "brpoplpush",
                "l",
                "dst",
                "0.01",
            ])))
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("$1\r\nb"));

        let bytes = rt
            .block_on(handler.apply_blocking_list_command(command(&["blpop", "missing", "0.001"])))
            .unwrap();
        assert_eq!(bytes, Frame::Null.as_bytes());

        rt.block_on(crate::command_dispatch::handle_command_async(
            &db,
            command(&["zadd", "z", "1", "one", "2", "two"]),
        ))
        .unwrap();
        let bytes = rt
            .block_on(handler.apply_blocking_zset_command(command(&["bzpopmin", "z", "0.01"])))
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("one"));
        let bytes = rt
            .block_on(handler.apply_blocking_zset_command(command(&[
                "bzmpop", "0.01", "1", "z", "max", "count", "1",
            ])))
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("two"));
        let bytes = rt
            .block_on(
                handler.apply_blocking_zset_command(command(&["bzpopmax", "missing", "0.001"])),
            )
            .unwrap();
        assert_eq!(bytes, Frame::Null.as_bytes());

        rt.block_on(crate::command_dispatch::handle_command_async(
            &db,
            command(&["xadd", "s", "1-0", "f", "v"]),
        ))
        .unwrap();
        let bytes = rt
            .block_on(handler.apply_blocking_stream_command(command(&[
                "xread", "block", "1", "streams", "s", "0-0",
            ])))
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("1-0"));
        let bytes = rt
            .block_on(handler.apply_blocking_stream_command(command(&[
                "xread", "block", "1", "streams", "empty", "0-0",
            ])))
            .unwrap();
        assert_eq!(bytes, Frame::Null.as_bytes());
    }

    #[test]
    fn blocking_once_handlers_cover_remaining_list_zset_and_stream_group_shapes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (handler, _) = rt.block_on(test_handler(1, None));
        let db = handler.get_session().get_db().clone();

        rt.block_on(crate::command_dispatch::handle_command_async(
            &db,
            command(&["rpush", "list", "a", "b", "c", "d"]),
        ))
        .unwrap();
        let frame = rt
            .block_on(handler.try_blocking_list_command_once(&command(&["brpop", "list", "0.01"])))
            .unwrap()
            .unwrap();
        assert!(matches!(
            frame,
            Frame::Array(values)
                if values.len() == 2 && matches!(&values[1], Frame::BulkString(value) if value == b"d")
        ));

        let frame = rt
            .block_on(handler.try_blocking_list_command_once(&command(&[
                "blmove", "list", "dst", "left", "right", "0.01",
            ])))
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Frame::BulkString(value) if value == b"a"));

        let frame = rt
            .block_on(handler.try_blocking_list_command_once(&command(&[
                "blmpop", "0.01", "2", "missing", "list", "left", "count", "2",
            ])))
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Frame::Array(values) if values.len() == 2));
        assert!(
            rt.block_on(handler.try_blocking_list_command_once(&command(&[
                "brpoplpush",
                "missing",
                "dst",
                "0.01",
            ])))
            .unwrap()
            .is_none()
        );

        rt.block_on(crate::command_dispatch::handle_command_async(
            &db,
            command(&["zadd", "myzset", "1", "one", "2", "two", "3", "three"]),
        ))
        .unwrap();
        let frame = rt
            .block_on(
                handler.try_blocking_zset_command_once(&command(&["bzpopmax", "myzset", "0.01"])),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            frame,
            Frame::Array(values)
                if matches!(&values[1], Frame::BulkString(value) if value == b"three")
        ));
        let frame = rt
            .block_on(handler.try_blocking_zset_command_once(&command(&[
                "bzmpop", "0.01", "1", "myzset", "min", "count", "2",
            ])))
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Frame::Array(values) if values.len() == 2));
        assert!(
            rt.block_on(
                handler.try_blocking_zset_command_once(&command(&["bzpopmin", "missing", "0.01",]))
            )
            .unwrap()
            .is_none()
        );

        let fields = vec![("f".to_string(), "v".to_string())];
        db.stream_add("stream", Some(StreamId { ms: 1, seq: 0 }), &fields)
            .unwrap();
        db.stream_group_create("stream", "group", StreamId { ms: 0, seq: 0 }, false)
            .unwrap();
        let frame = rt
            .block_on(handler.try_stream_read_once(&command(&[
                "xreadgroup",
                "group",
                "group",
                "consumer",
                "block",
                "1",
                "streams",
                "stream",
                ">",
            ])))
            .unwrap();
        assert!(matches!(frame, Frame::Array(values) if !values.is_empty()));
        let frame = rt
            .block_on(handler.try_stream_read_once(&command(&[
                "xreadgroup",
                "group",
                "group",
                "consumer",
                "block",
                "1",
                "streams",
                "stream",
                ">",
            ])))
            .unwrap();
        assert!(matches!(frame, Frame::Null));
    }

    #[test]
    fn transaction_async_dirty_watch_and_state_errors_are_reported() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut handler, _) = rt.block_on(test_handler(2, None));

        assert!(matches!(
            rt.block_on(handler.execute_transaction_async()).unwrap(),
            Frame::Error(message) if message.contains("without MULTI")
        ));

        handler.start_transaction().unwrap();
        handler.session.mark_transaction_dirty();
        assert!(matches!(
            rt.block_on(handler.execute_transaction_async()).unwrap(),
            Frame::Error(message) if message.contains("EXECABORT")
        ));

        handler.start_transaction().unwrap();
        let mut queued = Vec::new();
        handler.queue_transaction_frame(
            Frame::Array(vec![
                Frame::bulk_string("set"),
                Frame::bulk_string("k"),
                Frame::bulk_string("v"),
            ]),
            &mut queued,
        );
        assert_eq!(queued, b"+QUEUED\r\n");
        assert!(matches!(
            rt.block_on(handler.execute_transaction_async()).unwrap(),
            Frame::Array(values) if values.len() == 1
        ));

        handler.watch_keys(vec!["watched".to_string()]).unwrap();
        rt.block_on(
            crate::command_dispatch::handle_command_async(
                handler.get_session().get_db().as_ref(),
                command(&["set", "watched", "changed"]),
            ),
        )
        .unwrap();
        handler.start_transaction().unwrap();
        handler.add_transaction_frame(Frame::Array(vec![
            Frame::bulk_string("set"),
            Frame::bulk_string("after-watch"),
            Frame::bulk_string("v"),
        ]));
        assert!(matches!(
            rt.block_on(handler.execute_transaction_async()).unwrap(),
            Frame::Null
        ));
        assert!(
            !handler
                .get_session()
                .get_db()
                .exists_readonly("after-watch")
        );

        handler.start_transaction().unwrap();
        let mut rejected = Vec::new();
        handler.queue_transaction_frame(
            Frame::Array(vec![Frame::bulk_string("auth"), Frame::bulk_string("pw")]),
            &mut rejected,
        );
        assert!(String::from_utf8_lossy(&rejected).contains("not allowed"));
        assert!(matches!(
            rt.block_on(handler.execute_transaction_async()).unwrap(),
            Frame::Error(message) if message.contains("previous errors")
        ));
    }

    #[test]
    fn command_apply_routes_server_and_db_commands_without_full_tcp_loop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut handler, _) = rt.block_on(test_handler(2, Some("secret")));

        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["get", "missing"])))
                .unwrap(),
            Frame::Null
        ));
        assert!(handler.login(None, "secret").is_ok());
        assert!(
            matches!(rt.block_on(handler.apply_command(command(&["ping"]))).unwrap(), Frame::SimpleString(value) if value == "PONG")
        );
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["set", "k", "v"])))
                .unwrap(),
            Frame::Ok
        ));
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["get", "k"]))).unwrap(),
            Frame::BulkString(value) if value == b"v"
        ));
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["select", "1"])))
                .unwrap(),
            Frame::Ok
        ));
        assert_eq!(handler.get_session().get_current_db(), 1);
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["move", "k", "99"]))).unwrap(),
            Frame::Error(message) if message.contains("out of range")
        ));
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["flushall"])))
                .unwrap(),
            Frame::Ok
        ));
        assert!(matches!(
            rt.block_on(handler.apply_command(command(&["command-does-not-exist"]))).unwrap(),
            Frame::Error(message) if message.contains("unknown command")
        ));

        let (mut open_handler, _) = rt.block_on(test_handler(1, None));
        assert!(matches!(
            rt.block_on(open_handler.apply_command(command(&["multi"])))
                .unwrap(),
            Frame::Ok
        ));
        assert!(open_handler.is_in_transaction());
        assert!(matches!(
            rt.block_on(open_handler.apply_command(command(&["discard"])))
                .unwrap(),
            Frame::Ok
        ));
    }
