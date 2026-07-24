use super::super::*;
use super::support::*;

#[test]
fn aggregate_cursors_enforce_idle_and_memory_limits() {
    let cursor_id = register_fulltext_aggregate_cursor(
        0,
        "idx",
        vec![row("doc:1", 1.0, &[("title", "rust")])],
        1,
        usize::MAX,
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert!(read_fulltext_aggregate_cursor(0, "idx", cursor_id, 1).is_err());

    assert!(
        register_fulltext_aggregate_cursor(
            0,
            "idx",
            vec![row("doc:2", 1.0, &[("title", "rust")])],
            300_000,
            1,
        )
        .is_err()
    );
}
