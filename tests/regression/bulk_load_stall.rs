use std::collections::HashMap;
use std::time::Duration;

use tokio::time::timeout;
use worktable::prelude::PersistedWorkTable;
use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable!(
    name: BulkLoadStall,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String,
    },
    indexes: {
        test_idx: test unique,
        another_idx: another,
        exchange_idx: exchange,
    },
);

/// Exercises the persisted table through its public write API.
///
/// The unthrottled insert/delete workload makes the persistence batcher group
/// operations from different data pages. Before the CDC-gap fixes that could
/// kill the background persistence task with a TOC panic; `wait_for_ops` then
/// exposed the dead task only as a hang.
#[test]
fn public_bulk_insert_delete_survives_reload() {
    let config = DiskConfig::new_with_table_name(
        "tests/data/bulk_load_stall/persisted",
        BulkLoadStallWorkTable::name_snake_case(),
        BulkLoadStallWorkTable::version(),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/bulk_load_stall/persisted".to_string()).await;

        let mut rows = HashMap::new();
        {
            let engine = BulkLoadStallPersistenceEngine::new(config.clone()).await.unwrap();
            let table = BulkLoadStallWorkTable::load(engine).await.unwrap();

            for i in 0..1_000i64 {
                let row = BulkLoadStallRow {
                    id: table.get_next_pk().into(),
                    test: i,
                    another: i as u64,
                    exchange: format!("test{i}"),
                };
                let id = row.id;
                table.insert(row.clone()).unwrap();
                rows.insert(id, row);
            }

            let mut ids: Vec<_> = rows.keys().cloned().collect();
            ids.sort_unstable();
            let deleted: Vec<u64> = ids.into_iter().take(50).collect();
            for id in &deleted {
                table.delete(*id).await.unwrap();
            }

            timeout(Duration::from_secs(30), table.wait_for_ops())
                .await
                .expect("persistence stalled on public bulk insert/delete");

            for id in &deleted {
                rows.remove(id);
            }
        }

        let engine = BulkLoadStallPersistenceEngine::new(config).await.unwrap();
        let table = BulkLoadStallWorkTable::load(engine).await.unwrap();

        assert_eq!(table.count(), rows.len());
        for (id, expected) in &rows {
            assert_eq!(table.select(*id).as_ref(), Some(expected));
        }
    });
}
