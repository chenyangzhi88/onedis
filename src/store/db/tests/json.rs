use super::*;
use crate::store::db::is_packed_json_raw;
use crate::store::db::{JsonNodeSnapshot, JsonPathToken};

#[test]
fn update_preserves_ttl_for_get() {
    let db = test_db();

    db.insert("ttl-key".to_string(), Structure::String("v1".to_string()))
        .unwrap();
    db.expire("ttl-key".to_string(), 20).unwrap();
    db.update("ttl-key".to_string(), Structure::String("v2".to_string()))
        .unwrap();

    assert!(matches!(
        db.get("ttl-key").unwrap(),
        Some(Structure::String(value)) if value == "v2"
    ));

    sleep(Duration::from_millis(30));
    assert!(db.get("ttl-key").unwrap().is_none());
}

#[test]
fn json_set_get_type_and_del_paths() {
    let db = test_db();

    assert!(
        db.json_set(
            "doc",
            "$",
            r#"{"name":"alice","items":[1,2],"profile":{"city":"Paris"}}"#,
            SetCondition::Always,
        )
        .unwrap()
    );

    assert_eq!(db.json_type("doc", "$").unwrap(), Some("object"));
    assert_eq!(db.json_type("doc", "$.items").unwrap(), Some("array"));
    assert_eq!(db.json_type("doc", "$.items[0]").unwrap(), Some("integer"));
    assert_eq!(
        db.json_get("doc", "$.profile.city").unwrap(),
        Some(r#""Paris""#.to_string())
    );

    assert!(
        db.json_set("doc", "$.profile.city", r#""Berlin""#, SetCondition::Xx)
            .unwrap()
    );
    assert_eq!(
        db.json_get("doc", "$.profile").unwrap(),
        Some(r#"{"city":"Berlin"}"#.to_string())
    );

    assert!(
        db.json_set("doc", "$.profile.zip", "10115", SetCondition::Nx)
            .unwrap()
    );
    assert!(
        !db.json_set("doc", "$.profile.zip", "75000", SetCondition::Nx)
            .unwrap()
    );
    assert_eq!(db.json_del("doc", "$.profile.zip").unwrap(), 1);
    assert_eq!(db.json_del("doc", "$.profile.zip").unwrap(), 0);
    assert_eq!(db.json_get("doc", "$.profile.zip").unwrap(), None);
}

#[tokio::test]
async fn root_json_set_batch_keeps_last_valid_value_and_per_command_errors() {
    let db = test_db();
    db.insert_string("wrong-json-type".to_string(), "value".to_string(), None)
        .unwrap();

    let replies = db
        .json_set_root_batch_async(&[
            ("batch-json", r#"{"v":1}"#),
            ("batch-json", "not-json"),
            ("batch-json", r#"{"v":3,"nested":[1,2]}"#),
            ("wrong-json-type", r#"{"v":4}"#),
        ])
        .await;

    assert!(replies[0].is_ok());
    assert!(replies[1].is_err());
    assert!(replies[2].is_ok());
    assert!(
        replies[3]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("WRONGTYPE")
    );
    assert_eq!(
        db.json_get_async("batch-json", "$").await.unwrap(),
        Some(r#"{"nested":[1,2],"v":3}"#.to_string())
    );
    assert_eq!(
        db.get_string("wrong-json-type").unwrap().as_deref(),
        Some("value")
    );
}

#[tokio::test]
async fn root_json_set_batch_publishes_each_known_logical_key_once() {
    let db = test_db();
    let (key_version, db_version) = db.watch_version_snapshot("batch-json-watch").unwrap();

    let replies = db
        .json_set_root_batch_async(&[
            ("batch-json-watch", r#"{"v":1}"#),
            ("batch-json-watch", r#"{"v":2}"#),
        ])
        .await;

    assert!(replies.iter().all(Result::is_ok));
    assert!(
        db.watch_version_changed("batch-json-watch", key_version, db_version)
            .unwrap()
    );
    assert_eq!(
        db.mutation_tracker
            .key_version(db.db_index, &db.mk("batch-json-watch")),
        key_version + 1
    );
    db.release_watch("batch-json-watch");
}

#[tokio::test]
async fn root_json_set_batch_reports_build_failure_for_every_command_on_the_key() {
    let db = test_db();
    let oversized_key = "k".repeat(u16::MAX as usize + 1);
    let replies = db
        .json_set_root_batch_async(&[
            (&oversized_key, r#"{"v":1}"#),
            (&oversized_key, r#"{"v":2}"#),
        ])
        .await;

    assert_eq!(replies.len(), 2);
    assert!(replies.iter().all(Result::is_err));
    assert!(db.store.get_raw(&db.mk(&oversized_key)).unwrap().is_none());

    // The main metadata and root node fit, but the long child path does not. A failed per-key
    // builder must not leak the already-built prefix of that document into the global batch.
    let partial_key = "p".repeat(65_500);
    let long_field = "f".repeat(100);
    let json = format!(r#"{{"{long_field}":1}}"#);
    let replies = db
        .json_set_root_batch_async(&[(&partial_key, &json), (&partial_key, &json)])
        .await;
    assert!(replies.iter().all(Result::is_ok));
    assert!(is_packed_json_raw(
        &db.store.get_raw(&db.mk(&partial_key)).unwrap().unwrap()
    ));
}

#[test]
fn json_paths_reject_ambiguous_and_resource_amplifying_inputs() {
    assert!(parse_json_path("").is_err());
    assert!(parse_json_path("[0]").is_err());
    assert_eq!(
        parse_json_path(".[0]").unwrap(),
        vec![JsonPathToken::Index(0)]
    );
    assert!(parse_json_path(&format!("$.{}", "a.".repeat(128) + "a")).is_err());
    assert!(parse_json_path(&format!("$.{}", "a".repeat(4096))).is_err());
}

#[test]
fn json_partial_update_uses_indexed_subtree_storage() {
    let db = test_db();
    let padding = "x".repeat(17 * 1024);
    let document = format!(
        r#"{{"profile":{{"name":"alice","city":"Paris"}},"stats":{{"count":1}},"padding":"{padding}"}}"#
    );

    assert!(
        db.json_set("doc", "$", &document, SetCondition::Always,)
            .unwrap()
    );
    let raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let (_, version, structure) = super::decode_entry(&raw).unwrap();
    assert!(matches!(
        structure,
        Structure::Json(marker) if marker == super::JSON_INDEXED_MARKER
    ));

    let untouched_path = super::parse_json_path("$.profile.name").unwrap();
    let untouched_key = super::json_node_key(db.db_index, "doc", version, &untouched_path);
    let untouched_before = db.store.get_raw(&untouched_key).unwrap();

    assert!(
        db.json_set("doc", "$.stats.count", "2", SetCondition::Xx)
            .unwrap()
    );

    assert_eq!(db.store.get_raw(&untouched_key).unwrap(), untouched_before);
    let root: serde_json::Value =
        serde_json::from_str(&db.json_get("doc", "$").unwrap().unwrap()).unwrap();
    assert_eq!(root["profile"]["name"], "alice");
    assert_eq!(root["profile"]["city"], "Paris");
    assert_eq!(root["stats"]["count"], 2);
    assert!(matches!(
        db.get("doc").unwrap(),
        Some(Structure::Json(json)) if serde_json::from_str::<serde_json::Value>(&json).unwrap()["stats"]["count"] == 2
    ));
}

#[test]
fn json_array_delete_keeps_later_element_storage_keys_and_root_delete_cleans_nodes() {
    let db = test_db();
    let padding = "x".repeat(17 * 1024);
    let document =
        format!(r#"{{"tags":["a","b","c"],"profile":{{"name":"alice"}},"padding":"{padding}"}}"#);

    assert!(
        db.json_set("doc", "$", &document, SetCondition::Always,)
            .unwrap()
    );
    let raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let header = super::decode_meta_header(&raw).unwrap();
    let third_query = super::parse_json_path("$.tags[2]").unwrap();
    let third_storage = db
        .resolve_json_storage_path("doc", header.version, &third_query)
        .unwrap()
        .unwrap();
    let third_key = super::json_node_key(db.db_index, "doc", header.version, &third_storage);
    let third_before = db.store.get_raw(&third_key).unwrap();
    assert_eq!(db.json_del("doc", "$.tags[1]").unwrap(), 1);
    assert_eq!(
        db.json_get("doc", "$.tags").unwrap(),
        Some(r#"["a","c"]"#.to_string())
    );
    assert_eq!(
        db.json_get("doc", "$.tags[1]").unwrap(),
        Some(r#""c""#.to_string())
    );
    assert_eq!(db.store.get_raw(&third_key).unwrap(), third_before);

    let raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let header = super::decode_meta_header(&raw).unwrap();
    let node_prefix = super::json_node_prefix(db.db_index, "doc", header.version);
    assert!(!db.store.scan_prefix_raw(&node_prefix).unwrap().is_empty());
    assert_eq!(db.json_del("doc", "$").unwrap(), 1);
    assert!(db.store.scan_prefix_raw(&node_prefix).unwrap().is_empty());
}

#[tokio::test]
async fn json_sync_indexed_and_integer_string_edges_are_covered() {
    let db = test_db();

    db.insert(
        "legacy".to_string(),
        Structure::Json(r#"{"name":"alice","nested":{"x":1},"arr":[1,2]}"#.to_string()),
    )
    .unwrap();
    assert!(db.json_get("legacy", "$").is_err());
    assert!(
        db.json_set("legacy", "$", r#"{"blocked":true}"#, SetCondition::Always)
            .is_err()
    );
    assert!(db.json_del("legacy", "$").is_err());

    assert!(
        !db.json_set("missing-json", "$.a", "1", SetCondition::Always)
            .unwrap()
    );
    assert!(
        !db.json_set("missing-json", "$", "1", SetCondition::Xx)
            .unwrap()
    );
    assert!(
        db.json_set(
            "indexed",
            "$",
            r#"{"obj":{},"arr":[1]}"#,
            SetCondition::Always
        )
        .unwrap()
    );
    assert!(
        !db.json_set("indexed", "$", r#"{"blocked":true}"#, SetCondition::Nx)
            .unwrap()
    );
    assert!(
        db.json_set("indexed", "$.obj.name", r#""alice""#, SetCondition::Nx)
            .unwrap()
    );
    assert!(
        db.json_set("indexed", "$.arr[0]", "42", SetCondition::Xx)
            .unwrap()
    );
    assert!(
        !db.json_set("indexed", "$.arr[4]", "99", SetCondition::Always)
            .unwrap()
    );
    assert!(
        !db.json_set("indexed", "$.obj[0]", "99", SetCondition::Always)
            .unwrap()
    );
    assert!(
        db.json_set("indexed", "$", r#"{"root":true}"#, SetCondition::Always)
            .unwrap()
    );

    assert_eq!(
        db.update_integer_string("counter", |value| Some(value + 5))
            .unwrap(),
        5
    );
    assert_eq!(
        db.update_integer_string_async("counter", |value| Some(value + 1))
            .await
            .unwrap(),
        6
    );
    assert!(db.update_integer_string("counter", |_| None).is_err());
    assert_eq!(db.increment_integer_string("cached", 1).unwrap(), 1);
    assert_eq!(db.increment_integer_string("cached", 2).unwrap(), 3);
    assert_eq!(
        db.increment_integer_string_async("cached", 3)
            .await
            .unwrap(),
        6
    );
    db.insert_string("ttl-counter".to_string(), "10".to_string(), Some(20_000))
        .unwrap();
    assert_eq!(
        db.update_integer_string_async("ttl-counter", |value| Some(value + 1))
            .await
            .unwrap(),
        11
    );
    assert!(db.ttl_millis("ttl-counter").unwrap() > 0);

    db.insert_string_ref("ex", "value").unwrap();
    assert_eq!(
        db.getex_string_bytes("ex", None).unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        db.getex_string_bytes("ex", Some(StringExpireUpdate::RelativeMs(20_000)))
            .unwrap(),
        Some(b"value".to_vec())
    );
    assert!(db.ttl_millis("ex").unwrap() > 0);
    assert_eq!(
        db.getex_string_bytes_async("ex", Some(StringExpireUpdate::Persist))
            .await
            .unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(db.ttl_millis("ex").unwrap(), -1);
    assert_eq!(
        db.getex_string_bytes_async(
            "ex",
            Some(StringExpireUpdate::AbsoluteMs(
                now_ms().saturating_add(20_000),
            )),
        )
        .await
        .unwrap(),
        Some(b"value".to_vec())
    );
    assert!(db.ttl_millis("ex").unwrap() > 0);
    assert_eq!(
        db.getex_string_bytes("ex", Some(StringExpireUpdate::AbsoluteMs(1)))
            .unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(db.get_string("ex").unwrap(), None);
    assert_eq!(db.getex_string_bytes("missing", None).unwrap(), None);

    assert!(db.get_string_entry_raw_bytes(b"missing").unwrap().is_none());
    db.hash_set("not-string", "f", "v").unwrap();
    assert!(db.get_string_entry_raw_bytes(b"not-string").is_err());
    assert!(db.get_string_bytes("not-string").is_err());
    assert!(db.get_string_bytes_async("not-string").await.is_err());
    assert_eq!(db.type_name_readonly("not-string").unwrap(), "hash");
    assert_eq!(
        db.type_name_readonly_async("not-string").await.unwrap(),
        "hash"
    );
    assert!(db.exists_readonly_async("not-string").await.unwrap());
}

#[test]
fn json_batch_construction_errors_are_propagated_without_partial_entries() {
    let db = test_db();
    let oversized_key = "k".repeat(u16::MAX as usize + 1);
    let value: serde_json::Value = serde_json::from_str(r#"{"field":1}"#).unwrap();

    let mut subtree_batch = WriteBatch::new();
    let mut path = Vec::new();
    assert!(
        write_json_subtree_to_batch(
            &mut subtree_batch,
            db.db_index,
            &oversized_key,
            1,
            &mut path,
            &value,
        )
        .is_err()
    );
    assert_eq!(subtree_batch.count(), 0);

    let mut meta_batch = WriteBatch::new();
    assert!(
        db.touch_json_meta_to_batch(&mut meta_batch, &oversized_key, 0, 1)
            .is_err()
    );
    assert_eq!(meta_batch.count(), 0);
}

#[test]
fn json_wrong_type_is_rejected() {
    let db = test_db();

    db.insert_string_ref("doc", "plain").unwrap();

    assert_eq!(
        db.json_get("doc", "$").unwrap_err().to_string(),
        WRONG_TYPE_ERROR
    );
    assert_eq!(
        db.json_set("doc", "$", "{}", SetCondition::Always)
            .unwrap_err()
            .to_string(),
        WRONG_TYPE_ERROR
    );
}

#[tokio::test]
async fn json_command_async_path_uses_json_store() {
    let db = test_db();

    let frame = Command::JsonSet(JsonSet {
        key: "cart".to_string(),
        path: "$".to_string(),
        value: r#"{"total":10}"#.to_string(),
        condition: SetCondition::Always,
    });

    assert!(matches!(
        crate::command_dispatch::handle_command_async(&db, frame)
            .await
            .unwrap(),
        crate::frame::Frame::Ok
    ));
    assert_eq!(
        db.json_get_async("cart", "$.total").await.unwrap(),
        Some("10".to_string())
    );
}

#[tokio::test]
async fn json_indexed_async_updates_deletes_and_conditions_cover_edges() {
    let db = test_db();
    let padding = "x".repeat(17 * 1024);
    let document = format!(
        r#"{{"profile":{{"name":"alice","city":"Paris"}},"tags":["a","b","c"],"stats":{{"count":1}},"padding":"{padding}"}}"#
    );

    assert!(
        db.json_set_async("doc", "$", &document, SetCondition::Always,)
            .await
            .unwrap()
    );
    let raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let (_, _, structure) = super::decode_entry(&raw).unwrap();
    assert!(matches!(
        structure,
        Structure::Json(marker) if marker == super::JSON_INDEXED_MARKER
    ));

    assert_eq!(
        db.json_type_async("doc", "$.profile").await.unwrap(),
        Some("object")
    );
    assert_eq!(
        db.json_type_async("doc", "$.tags").await.unwrap(),
        Some("array")
    );
    assert!(
        !db.json_set_async("doc", "$.stats.count", "2", SetCondition::Nx)
            .await
            .unwrap()
    );
    assert!(
        db.json_set_async("doc", "$.stats.count", "2", SetCondition::Xx)
            .await
            .unwrap()
    );
    assert!(
        db.json_set_async("doc", "$.profile.zip", "10115", SetCondition::Nx)
            .await
            .unwrap()
    );
    assert_eq!(
        db.json_get_async("doc", "$.profile.zip").await.unwrap(),
        Some("10115".to_string())
    );

    assert_eq!(db.json_del_async("doc", "$.tags[1]").await.unwrap(), 1);
    assert_eq!(
        db.json_get_async("doc", "$.tags").await.unwrap(),
        Some(r#"["a","c"]"#.to_string())
    );
    assert_eq!(db.json_del_async("doc", "$.profile.zip").await.unwrap(), 1);
    assert_eq!(db.json_del_async("doc", "$.profile.zip").await.unwrap(), 0);
    assert_eq!(db.json_del_async("doc", "$.missing").await.unwrap(), 0);
    assert_eq!(db.json_del_async("missing", "$").await.unwrap(), 0);
    assert_eq!(db.json_del_async("doc", "$").await.unwrap(), 1);
    assert_eq!(db.json_get_async("doc", "$").await.unwrap(), None);
}

#[tokio::test]
async fn concurrent_json_set_async_keeps_all_object_fields() {
    let db = Arc::new(test_db());
    db.json_set_async("doc", "$", r#"{"fields":{}}"#, SetCondition::Always)
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for idx in 0..64 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let path = format!("$.fields.f{idx}");
            db.json_set_async("doc", &path, &idx.to_string(), SetCondition::Nx)
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let fields: serde_json::Value =
        serde_json::from_str(&db.json_get_async("doc", "$.fields").await.unwrap().unwrap())
            .unwrap();
    assert_eq!(fields.as_object().unwrap().len(), 64);
    for idx in 0..64 {
        assert_eq!(fields[format!("f{idx}")], idx);
    }
}

#[tokio::test]
async fn concurrent_json_updates_preserve_independent_paths_and_watch_visibility() {
    let db = Arc::new(test_db());
    db.json_set_async(
        "doc",
        "$",
        r#"{"left":{"value":0},"right":{"value":0}}"#,
        SetCondition::Always,
    )
    .await
    .unwrap();
    let snapshot = db.watch_version_snapshot("doc").unwrap();

    let left_db = db.clone();
    let left = tokio::spawn(async move {
        for value in 1..=64 {
            assert!(
                left_db
                    .json_set_async("doc", "$.left.value", &value.to_string(), SetCondition::Xx,)
                    .await
                    .unwrap()
            );
        }
    });
    let right_db = db.clone();
    let right = tokio::spawn(async move {
        for value in 1..=64 {
            assert!(
                right_db
                    .json_set_async("doc", "$.right.value", &value.to_string(), SetCondition::Xx,)
                    .await
                    .unwrap()
            );
        }
    });
    left.await.unwrap();
    right.await.unwrap();

    assert_eq!(
        db.json_get_async("doc", "$.left.value").await.unwrap(),
        Some("64".into())
    );
    assert_eq!(
        db.json_get_async("doc", "$.right.value").await.unwrap(),
        Some("64".into())
    );
    assert!(
        db.watch_version_changed("doc", snapshot.0, snapshot.1)
            .unwrap()
    );
    db.release_watch("doc");
}

#[tokio::test]
async fn concurrent_json_ancestor_replacement_and_descendant_updates_stay_consistent() {
    let db = Arc::new(test_db());
    let padding = "x".repeat(17 * 1024);
    let document =
        format!(r#"{{"branch":{{"leaf":{{"value":0}},"other":1}},"padding":"{padding}"}}"#);
    db.json_set_async("doc", "$", &document, SetCondition::Always)
        .await
        .unwrap();

    let replace_db = db.clone();
    let replace = tokio::spawn(async move {
        for value in 1..=32 {
            replace_db
                .json_set_async(
                    "doc",
                    "$.branch",
                    &format!(r#"{{"leaf":{{"value":{value}}},"other":{value}}}"#),
                    SetCondition::Xx,
                )
                .await
                .unwrap();
        }
    });
    let leaf_db = db.clone();
    let leaf = tokio::spawn(async move {
        for value in 100..132 {
            leaf_db
                .json_set_async(
                    "doc",
                    "$.branch.leaf.value",
                    &value.to_string(),
                    SetCondition::Xx,
                )
                .await
                .unwrap();
        }
    });
    replace.await.unwrap();
    leaf.await.unwrap();

    let document: serde_json::Value =
        serde_json::from_str(&db.json_get_async("doc", "$").await.unwrap().unwrap()).unwrap();
    assert!(document["branch"]["leaf"]["value"].is_number());
    assert!(document["branch"]["other"].is_number());

    let raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let version = super::decode_meta_header(&raw).unwrap().version;
    let prefix = super::json_node_prefix(db.db_index, "doc", version);
    let entries = db.store.scan_prefix_raw(&prefix).unwrap();
    let snapshot = JsonNodeSnapshot::from_entries(&prefix, entries).unwrap();
    assert_eq!(
        snapshot.value_at(&mut Vec::new()).unwrap().unwrap(),
        document
    );
}

#[test]
fn json_root_replacement_switches_version_without_eager_old_subtree_delete() {
    let db = test_db();
    let padding = "x".repeat(17 * 1024);
    let document = format!(r#"{{"wide":{{"a":1,"b":2}},"items":[1,2,3],"padding":"{padding}"}}"#);
    db.json_set("doc", "$", &document, SetCondition::Always)
        .unwrap();
    let old_raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let old_version = super::decode_meta_header(&old_raw).unwrap().version;
    let old_prefix = super::json_node_prefix(db.db_index, "doc", old_version);
    let old_count = db.store.scan_prefix_raw(&old_prefix).unwrap().len();

    db.json_set("doc", "$", r#"{"next":true}"#, SetCondition::Always)
        .unwrap();
    let new_raw = db.store.get_raw(&db.mk("doc")).unwrap().unwrap();
    let new_version = super::decode_meta_header(&new_raw).unwrap().version;
    assert_ne!(new_version, old_version);
    assert_eq!(
        db.store.scan_prefix_raw(&old_prefix).unwrap().len(),
        old_count
    );
    assert_eq!(db.refresh_retired_versions_once(usize::MAX), 1);
    assert_eq!(
        db.json_get("doc", "$").unwrap(),
        Some(r#"{"next":true}"#.into())
    );
}
