use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: LockLifecycle,
    columns: {
        id: u64 primary_key,
        value: u64,
        other: u64,
    }
);

#[tokio::test]
async fn retained_row_lock_arc_prevents_cleanup_replacement() {
    let table = LockLifecycleWorkTable::default();
    let pk = LockLifecyclePrimaryKey(7);
    let blocker = Arc::new(Lock::new(u16::MAX));
    let mut blocker_state = LockLifecycleLock::new();
    blocker_state.value_lock = Some(blocker.clone());
    blocker_state.other_lock = Some(blocker.clone());
    blocker_state.id_lock = Some(blocker.clone());
    table
        .0
        .lock_manager
        .insert(pk.clone(), Arc::new(tokio::sync::RwLock::new(blocker_state)));
    let retained = table.0.lock_manager.get(&pk).expect("blocker should be registered");

    {
        let _guard = LockGuard::new(blocker, table.0.lock_manager.clone(), pk.clone());
    }

    let reacquired = table.0.lock_manager.get_or_insert(pk.clone());
    assert!(Arc::ptr_eq(&retained, &reacquired));
}
