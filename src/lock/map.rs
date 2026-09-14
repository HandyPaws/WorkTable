use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use parking_lot::RwLock;

use crate::lock::RowLock;

#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey> {
    map: RwLock<HashMap<PrimaryKey, Arc<tokio::sync::RwLock<LockType>>>>,
    next_id: AtomicU16,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<tokio::sync::RwLock<LockType>>,
    ) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.write().insert(key, lock)
    }

    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.read().get(key).cloned()
    }

    /// Returns the row lock registered for `key`, creating an empty one when absent.
    ///
    /// The read fast path clones under the map read lock. The write path uses
    /// `entry` so the lookup and first insertion stay atomic.
    pub fn get_or_insert(&self, key: PrimaryKey) -> Arc<tokio::sync::RwLock<LockType>>
    where
        LockType: RowLock,
    {
        if let Some(lock) = self.map.read().get(&key) {
            return lock.clone();
        }

        let mut map = self.map.write();
        map.entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(<LockType as RowLock>::new())))
            .clone()
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.map.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.map.write();
        if let Some(lock) = set.get(key).cloned()
            && let Ok(guard) = lock.try_read()
            && !guard.is_locked()
            // The map entry and this cleanup clone account for two strong
            // references. Any extra reference belongs to a caller that has
            // acquired the row object and may still register an operation.
            && Arc::strong_count(&lock) == 2
        {
            set.remove(key);
        }
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::LockMap;
    use crate::lock::FullRowLock;

    #[test]
    fn get_or_insert_returns_one_lock_for_concurrent_misses() {
        let lock_map: Arc<LockMap<FullRowLock, u64>> = Arc::new(LockMap::default());
        let barrier = Arc::new(Barrier::new(8));

        std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let lock_map = lock_map.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        let lock = lock_map.get_or_insert(1);
                        Arc::as_ptr(&lock) as usize
                    })
                })
                .collect::<Vec<_>>();

            let pointers = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            assert!(pointers.iter().all(|pointer| *pointer == pointers[0]));
        });
    }
}
