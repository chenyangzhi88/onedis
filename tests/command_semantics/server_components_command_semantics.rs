mod support;

use support::*;

#[test]
fn command_executor_enforces_max_in_flight_across_concurrent_work() {
    let executor = Arc::new(CommandExecutor::new(2, 2).unwrap());
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut results = rt.block_on(async {
        let mut tasks = Vec::new();
        for i in 0..8usize {
            let executor = executor.clone();
            let active = active.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                executor
                    .execute(async move {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        i
                    })
                    .await
                    .unwrap()
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        results
    });
    results.sort_unstable();
    assert_eq!(results, (0..8).collect::<Vec<_>>());
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

#[test]
fn command_executor_from_env_executes_work_with_default_limits() {
    let executor = CommandExecutor::from_env().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt
        .block_on(executor.execute(async { "executor-result".to_string() }))
        .unwrap();
    assert_eq!(result, "executor-result");
}

#[test]
fn database_manager_accessors_and_waiter_notifications_are_wired() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let args = test_args_with_config(3, None);
    let manager = rt.block_on(DatabaseManager::new_async(args.clone()));

    assert_eq!(manager.get_all_dbs().len(), 3);
    manager
        .get_db(2)
        .insert_string("db2-only".to_string(), "value".to_string(), None);
    assert!(manager.get_db(0).get_string("db2-only").unwrap().is_none());
    assert_eq!(
        manager.get_db(2).get_string("db2-only").unwrap(),
        Some("value".to_string())
    );
    assert!(manager.options().db_path.ends_with("db"));
    assert!(manager.options().wal_dir.ends_with("wal"));
    assert!(Arc::strong_count(manager.version_counter()) >= 4);
    assert!(Arc::strong_count(manager.ttl_manager()) >= 4);
    manager.store().put_raw(b"manager-accessor:key", b"value");
    assert_eq!(
        manager.store().get_raw(b"manager-accessor:key"),
        Some(b"value".to_vec())
    );

    assert!(rt.block_on(async {
        let list_notified = manager.list_notify().notified();
        manager.notify_list_waiters();
        tokio::time::timeout(Duration::from_millis(50), list_notified)
            .await
            .is_ok()
    }));

    assert!(rt.block_on(async {
        let zset_notified = manager.zset_notify().notified();
        manager.notify_zset_waiters();
        tokio::time::timeout(Duration::from_millis(50), zset_notified)
            .await
            .is_ok()
    }));

    assert!(rt.block_on(async {
        let stream_notified = manager.stream_notify().notified();
        manager.notify_stream_waiters();
        tokio::time::timeout(Duration::from_millis(50), stream_notified)
            .await
            .is_ok()
    }));
}

#[test]
fn handler_public_accessors_login_acl_and_client_name_state() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let args = test_args_with_config(2, Some("secret"));
    let db_manager = Arc::new(rt.block_on(DatabaseManager::new_async(args.clone())));
    let session_manager = Arc::new(SessionManager::new());
    let command_executor = Arc::new(CommandExecutor::new(1, 8).unwrap());
    let wasm_registry = Arc::new(WasmRegistry::new());
    let (server_stream, _client_stream) = rt.block_on(connected_streams());
    let mut handler = Handler::new(
        db_manager.clone(),
        session_manager.clone(),
        command_executor,
        wasm_registry,
        server_stream,
        args.clone(),
    );

    assert!(Arc::ptr_eq(handler.get_db_manager(), &db_manager));
    assert!(Arc::ptr_eq(handler.get_session_manager(), &session_manager));
    assert_eq!(handler.get_args().databases, 2);
    assert_eq!(handler.client_name(), None);
    handler.set_client_name(Some("frontend-debugger".to_string()));
    assert_eq!(handler.client_name(), Some("frontend-debugger".to_string()));
    handler.set_client_name(None);
    assert_eq!(handler.client_name(), None);

    assert!(!handler.get_session().get_certification());
    assert!(handler.login(None, "bad").is_err());
    assert!(handler.login(None, "secret").is_ok());
    assert!(handler.get_session().get_certification());
    assert_eq!(handler.get_session().user(), "default");

    session_manager
        .acl_setuser(
            "limited",
            &[
                "on".to_string(),
                ">pw".to_string(),
                "-@all".to_string(),
                "+get".to_string(),
            ],
        )
        .unwrap();
    assert!(handler.login(Some("limited"), "bad").is_err());
    assert!(handler.login(Some("limited"), "pw").is_ok());
    assert!(handler.get_session().get_certification());
    assert_eq!(handler.get_session().user(), "limited");

    assert!(handler.change_db(1).is_ok());
    assert_eq!(handler.get_session().get_current_db(), 1);
    assert!(handler.change_db(2).is_err());
}

#[tokio::test]
async fn session_manager_tracks_sessions_acl_and_pubsub_lifecycle() {
    let db = Arc::new(test_db());
    let manager = SessionManager::new();
    let mut session = Session::new(true, db.clone());
    session.set_current_db(2);
    session.set_name(Some("client-a".to_string()));
    session.set_last_cmd("get".to_string());
    let session_id = session.get_id();
    manager.create_session(&session);

    assert_eq!(manager.get_connection_count(), 1);
    assert!(manager.is_over_max_clients(1));
    let clients = manager.client_list();
    assert!(clients.contains(&format!("id={session_id}")));
    assert!(clients.contains("name=client-a"));
    assert!(clients.contains("db=2"));
    assert!(clients.contains("cmd=get"));

    session.set_current_db(3);
    session.set_user("limited".to_string());
    manager.update_session(&session);
    assert_eq!(manager.acl_whoami(session_id), "limited");
    assert!(manager.acl_authenticate("default", ""));
    assert!(
        manager
            .acl_setuser(
                "limited",
                &[
                    "on".to_string(),
                    ">secret".to_string(),
                    "-@all".to_string(),
                    "+get".to_string(),
                    "-set".to_string(),
                ],
            )
            .is_ok()
    );
    assert!(manager.acl_authenticate("limited", "secret"));
    assert!(manager.acl_allows("limited", "GET"));
    assert!(!manager.acl_allows("limited", "SET"));
    assert_eq!(
        manager.acl_deluser(&["limited".to_string(), "default".to_string()]),
        1
    );
    assert!(!manager.acl_authenticate("limited", "secret"));

    let (server_stream, mut client_stream) = connected_streams().await;
    let mut connection = Connection::new(server_stream);
    let writer = connection.shared_writer();
    manager.register_channel("news", session_id, writer.clone());
    manager.register_pattern("n*", session_id, writer.clone());
    manager.register_shard_channel("news", session_id, writer);
    let client_info = manager.client_info(session_id).unwrap();
    assert!(client_info.contains("sub=1"));
    assert!(client_info.contains("psub=1"));
    assert!(client_info.contains("ssub=1"));
    assert!(client_info.contains("flags=P"));
    assert!(client_info.contains("user=limited"));
    assert_eq!(manager.channel_count("news", false), 1);
    assert_eq!(manager.channel_count("news", true), 1);
    assert_eq!(manager.pattern_count(), 1);
    assert!(manager.channel_names(false).contains(&"news".to_string()));

    assert_eq!(manager.publish("news", "hello", false), 2);
    let mut buf = vec![0; 256];
    let read = client_stream.read(&mut buf).await.unwrap();
    let payload = String::from_utf8_lossy(&buf[..read]);
    assert!(payload.contains("message"));
    assert!(payload.contains("pmessage"));
    assert!(payload.contains("hello"));

    manager.unsubscribe_all(session_id);
    assert_eq!(manager.channel_count("news", false), 0);
    assert_eq!(manager.channel_count("news", true), 0);
    assert_eq!(manager.pattern_count(), 0);
    assert!(manager.remove_session(session_id));
    assert_eq!(manager.get_connection_count(), 0);
}

#[tokio::test]
async fn connection_reads_partial_frames_and_writes_resp_bytes() {
    let (server_stream, mut client_stream) = connected_streams().await;
    let mut connection = Connection::new(server_stream);

    client_stream.write_all(b"*1\r\n$4\r\nPI").await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    client_stream.write_all(b"NG\r\n").await.unwrap();

    let bytes = connection.read_bytes().await.unwrap();
    let frames = Frame::parse_multiple_frames(&bytes).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        Frame::Array(values)
            if values.len() == 1 && matches!(&values[0], Frame::BulkString(value) if value == b"PING")
    ));

    client_stream
        .write_all(b"*1\r\n$4\r\nECHO\r\n*1\r\n$4\r\nPING\r\n")
        .await
        .unwrap();
    let bytes = connection.read_bytes().await.unwrap();
    let frames = Frame::parse_multiple_frames(&bytes).unwrap();
    assert_eq!(frames.len(), 2);

    connection
        .write_bytes(Frame::SimpleString("OK".to_string()).as_bytes())
        .await;
    let mut response = vec![0; 16];
    let read = client_stream.read(&mut response).await.unwrap();
    assert_eq!(&response[..read], b"+OK\r\n");
}

#[tokio::test]
async fn wasm_commands_cover_parse_list_delete_and_missing_module_errors() {
    assert!(WasmCommand::parse_from_frame(frame_args(&["wasm.load", "only-name"])).is_err());
    assert!(WasmCommand::parse_from_frame(frame_args(&["wasm.scan", "m", "f"])).is_err());
    assert!(
        WasmCommand::parse_from_frame(frame_args(&["wasm.scan", "m", "f", "p", "bad"])).is_err()
    );
    assert!(WasmCommand::parse_from_frame(frame_args(&["function"])).is_err());
    assert!(WasmCommand::parse_from_frame(frame_args(&["function", "list", "extra"])).is_err());
    assert!(WasmCommand::parse_from_frame(frame_args(&["fcall", "badname", "0"])).is_err());
    assert!(WasmCommand::parse_from_frame(frame_args(&["fcall", "mod.func", "bad"])).is_err());

    let registry = Arc::new(WasmRegistry::new());
    let db = Arc::new(test_db());

    let list = WasmCommand::parse_from_frame(frame_args(&["wasm.list"]))
        .unwrap()
        .apply(&registry, db.clone())
        .await;
    assert!(matches!(list, Frame::Array(values) if values.is_empty()));

    let function_list = WasmCommand::parse_from_frame(frame_args(&["function", "list"]))
        .unwrap()
        .apply(&registry, db.clone())
        .await;
    assert!(matches!(function_list, Frame::Array(values) if values.is_empty()));

    let delete_missing =
        WasmCommand::parse_from_frame(frame_args(&["function", "delete", "missing"]))
            .unwrap()
            .apply(&registry, db.clone())
            .await;
    assert!(matches!(delete_missing, Frame::Integer(0)));

    let invalid_load = WasmCommand::parse_from_frame(Frame::Array(vec![
        Frame::bulk_string("wasm.load"),
        Frame::bulk_string("bad"),
        Frame::BulkString(b"not wasm".to_vec()),
    ]))
    .unwrap()
    .apply(&registry, db.clone())
    .await;
    assert!(matches!(invalid_load, Frame::Error(message) if message.contains("compile failed")));

    let missing_call = WasmCommand::parse_from_frame(frame_args(&["wasm.call", "missing", "run"]))
        .unwrap()
        .apply(&registry, db.clone())
        .await;
    assert!(matches!(missing_call, Frame::Error(message) if message.contains("module not found")));

    let missing_scan = WasmCommand::parse_from_frame(frame_args(&[
        "wasm.scan",
        "missing",
        "filter",
        "prefix",
        "5",
    ]))
    .unwrap()
    .apply(&registry, db.clone())
    .await;
    assert!(matches!(missing_scan, Frame::Error(message) if message.contains("module not found")));

    let fcall_ro =
        WasmCommand::parse_from_frame(frame_args(&["fcall_ro", "missing.run", "1", "key", "arg"]))
            .unwrap()
            .apply(&registry, db)
            .await;
    assert!(matches!(fcall_ro, Frame::Error(message) if message.contains("module not found")));
}

#[test]
fn auth_command_authenticates_handler_session() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let args = test_args_with_requirepass("secret");
    let db_manager = Arc::new(rt.block_on(DatabaseManager::new_async(args.clone())));
    let session_manager = Arc::new(SessionManager::new());
    let command_executor = Arc::new(CommandExecutor::new(1, 8).unwrap());
    let wasm_registry = Arc::new(WasmRegistry::new());
    let (server_stream, _client_stream) = rt.block_on(connected_streams());
    let mut handler = Handler::new(
        db_manager.clone(),
        session_manager,
        command_executor,
        wasm_registry,
        server_stream,
        args,
    );

    assert!(!handler.get_session().get_certification());
    let command = Command::parse_from_frame(frame_args(&["auth", "secret"])).unwrap();
    let frame = match command {
        Command::Auth(auth) => auth.apply(&mut handler).unwrap(),
        other => panic!("expected AUTH command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));
    assert!(handler.get_session().get_certification());
}

#[test]
fn connect_commands_cover_reply_and_client_name_semantics() {
    let ping = Command::parse_from_frame(frame_args(&["ping"])).unwrap();
    let frame = match ping {
        Command::Ping(command) => command.apply().unwrap(),
        other => panic!("expected PING command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::SimpleString(value) if value == "PONG"));

    let ping = Command::parse_from_frame(frame_args(&["ping", "hello"])).unwrap();
    let frame = match ping {
        Command::Ping(command) => command.apply().unwrap(),
        other => panic!("expected PING command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::BulkString(value) if value == b"hello"));

    let echo = Command::parse_from_frame(frame_args(&["echo", "payload"])).unwrap();
    let frame = match echo {
        Command::Echo(command) => command.apply().unwrap(),
        other => panic!("expected ECHO command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::BulkString(value) if value == b"payload"));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let args = test_args_with_config(2, None);
    let db_manager = Arc::new(rt.block_on(DatabaseManager::new_async(args.clone())));
    let session_manager = Arc::new(SessionManager::new());
    let command_executor = Arc::new(CommandExecutor::new(1, 8).unwrap());
    let wasm_registry = Arc::new(WasmRegistry::new());
    let (server_stream, _client_stream) = rt.block_on(connected_streams());
    let mut handler = Handler::new(
        db_manager.clone(),
        session_manager,
        command_executor,
        wasm_registry,
        server_stream,
        args,
    );

    let set_name =
        Command::parse_from_frame(frame_args(&["client", "setname", "worker-1"])).unwrap();
    let frame = match set_name {
        Command::Client(command) => command.apply_with_handler(&mut handler).unwrap(),
        other => panic!("expected CLIENT command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));

    let get_name = Command::parse_from_frame(frame_args(&["client", "getname"])).unwrap();
    let frame = match get_name {
        Command::Client(command) => command.apply_with_handler(&mut handler).unwrap(),
        other => panic!("expected CLIENT command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::BulkString(value) if value == b"worker-1"));

    let select = Command::parse_from_frame(frame_args(&["select", "1"])).unwrap();
    let frame = match select {
        Command::Select(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected SELECT command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));
    assert_eq!(handler.get_session().get_current_db(), 1);

    let invalid_select = Command::parse_from_frame(frame_args(&["select", "2"])).unwrap();
    let frame = match invalid_select {
        Command::Select(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected SELECT command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Error(message) if message.contains("out of range")));

    db_manager
        .get_db(1)
        .insert_string("copy-source".to_string(), "one".to_string(), None);
    let copy = Command::parse_from_frame(frame_args(&[
        "copy",
        "copy-source",
        "copy-destination",
        "db",
        "0",
    ]))
    .unwrap();
    let frame = match copy {
        Command::Copy(command) => {
            assert_eq!(command.source(), "copy-source");
            assert_eq!(command.destination(), "copy-destination");
            assert_eq!(command.db_index(), Some(0));
            assert!(!command.replace());
            command.apply_sync(&handler).unwrap()
        }
        other => panic!("expected COPY command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(1)));
    assert_eq!(
        db_manager.get_db(0).get_string("copy-destination").unwrap(),
        Some("one".to_string())
    );

    let copy_existing = Command::parse_from_frame(frame_args(&[
        "copy",
        "copy-source",
        "copy-destination",
        "db",
        "0",
    ]))
    .unwrap();
    let frame = match copy_existing {
        Command::Copy(command) => command.apply_sync(&handler).unwrap(),
        other => panic!("expected COPY command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(0)));

    let copy_replace = Command::parse_from_frame(frame_args(&[
        "copy",
        "copy-source",
        "copy-destination",
        "replace",
        "db",
        "0",
    ]))
    .unwrap();
    let frame = match copy_replace {
        Command::Copy(command) => {
            assert!(command.replace());
            command.apply_sync(&handler).unwrap()
        }
        other => panic!("expected COPY command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(1)));

    let copy_bad_db =
        Command::parse_from_frame(frame_args(&["copy", "copy-source", "x", "db", "2"])).unwrap();
    let frame = match copy_bad_db {
        Command::Copy(command) => command.apply_sync(&handler).unwrap(),
        other => panic!("expected COPY command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Error(message) if message.contains("out of range")));
    assert!(Command::parse_from_frame(frame_args(&["copy", "a"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["copy", "a", "b", "db"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["copy", "a", "b", "db", "bad"])).is_err());
    assert!(
        Command::parse_from_frame(frame_args(&["copy", "a", "b", "replace", "replace"])).is_err()
    );

    let move_same_db =
        Command::parse_from_frame(frame_args(&["move", "copy-source", "1"])).unwrap();
    let frame = match move_same_db {
        Command::Move(command) => {
            assert_eq!(command.get_key(), "copy-source");
            assert_eq!(command.get_db_index(), 1);
            command.apply_sync(&handler).unwrap()
        }
        other => panic!("expected MOVE command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(0)));

    let move_cross_db =
        Command::parse_from_frame(frame_args(&["move", "copy-source", "0"])).unwrap();
    let frame = match move_cross_db {
        Command::Move(command) => command.apply_sync(&handler).unwrap(),
        other => panic!("expected MOVE command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(1)));
    assert_eq!(
        db_manager.get_db(0).get_string("copy-source").unwrap(),
        Some("one".to_string())
    );
    assert!(
        db_manager
            .get_db(1)
            .get_string("copy-source")
            .unwrap()
            .is_none()
    );

    let move_missing =
        Command::parse_from_frame(frame_args(&["move", "copy-source", "0"])).unwrap();
    let frame = match move_missing {
        Command::Move(command) => command.apply_sync(&handler).unwrap(),
        other => panic!("expected MOVE command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Integer(0)));

    let move_bad_db = Command::parse_from_frame(frame_args(&["move", "copy-source", "2"])).unwrap();
    let frame = match move_bad_db {
        Command::Move(command) => command.apply_sync(&handler).unwrap(),
        other => panic!("expected MOVE command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Error(message) if message.contains("out of range")));
    assert!(Command::parse_from_frame(frame_args(&["move", "k"])).is_err());
    assert!(Command::parse_from_frame(frame_args(&["move", "k", "bad"])).is_err());
}

#[test]
fn transaction_commands_cover_state_and_watch_semantics() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let args = test_args_with_config(1, None);
    let db_manager = Arc::new(rt.block_on(DatabaseManager::new_async(args.clone())));
    let session_manager = Arc::new(SessionManager::new());
    let command_executor = Arc::new(CommandExecutor::new(1, 8).unwrap());
    let wasm_registry = Arc::new(WasmRegistry::new());
    let (server_stream, _client_stream) = rt.block_on(connected_streams());
    let mut handler = Handler::new(
        db_manager,
        session_manager,
        command_executor,
        wasm_registry,
        server_stream,
        args,
    );

    let discard = Command::parse_from_frame(frame_args(&["discard"])).unwrap();
    let frame = match discard {
        Command::Discard(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected DISCARD command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Error(message) if message.contains("without MULTI")));

    let watch = Command::parse_from_frame(frame_args(&["watch", "watched-key"])).unwrap();
    let frame = match watch {
        Command::Watch(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected WATCH command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));

    let unwatch = Command::parse_from_frame(frame_args(&["unwatch"])).unwrap();
    let frame = match unwatch {
        Command::Unwatch(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected UNWATCH command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));

    let multi = Command::parse_from_frame(frame_args(&["multi"])).unwrap();
    let frame = match multi {
        Command::Multi(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected MULTI command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));
    assert!(handler.is_in_transaction());

    let watch_inside_multi = Command::parse_from_frame(frame_args(&["watch", "late-key"])).unwrap();
    let err = match watch_inside_multi {
        Command::Watch(command) => match command.apply(&mut handler) {
            Ok(frame) => panic!(
                "expected WATCH inside MULTI error, got {}",
                frame.to_string()
            ),
            Err(err) => err,
        },
        other => panic!("expected WATCH command, got {}", other.name()),
    };
    assert!(err.to_string().contains("inside MULTI"));

    let exec = Command::parse_from_frame(frame_args(&["exec"])).unwrap();
    let frame = match exec {
        Command::Exec(command) => command.apply().unwrap(),
        other => panic!("expected EXEC command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));

    let discard = Command::parse_from_frame(frame_args(&["discard"])).unwrap();
    let frame = match discard {
        Command::Discard(command) => command.apply(&mut handler).unwrap(),
        other => panic!("expected DISCARD command, got {}", other.name()),
    };
    assert!(matches!(frame, Frame::Ok));
    assert!(!handler.is_in_transaction());
}

#[test]
fn save_and_bgsave_force_kv_engine_flush_paths() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let args = test_args_with_config(1, None);
    let db_manager = rt.block_on(DatabaseManager::new_async(args));

    let save = Save::parse_from_frame(frame_args(&["save"])).unwrap();
    assert!(matches!(save.apply_sync(&db_manager).unwrap(), Frame::Ok));

    let bgsave = Bgsave::parse_from_frame(frame_args(&["bgsave"])).unwrap();
    assert!(matches!(bgsave.apply_sync(&db_manager).unwrap(), Frame::Ok));
}

#[test]
fn append_uses_kv_engine_backed_storage() {
    let db = test_db();
    db.insert(
        "greeting".to_string(),
        Structure::String("hello".to_string()),
    );

    let frame = Append {
        key: "greeting".to_string(),
        val: " world".to_string(),
    }
    .apply(&db)
    .unwrap();

    assert!(matches!(frame, Frame::Integer(11)));
    assert!(matches!(
        db.get("greeting"),
        Some(Structure::String(value)) if value == "hello world"
    ));
}
