use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Maps conversation thread IDs to their last-used backend index.
///
/// Affinity is soft: if the preferred backend is unavailable the fallback
/// chain picks a different one and updates the map so subsequent requests
/// follow the new backend rather than continuously retrying the broken one.
#[derive(Clone)]
pub struct SessionAffinity {
    inner: Arc<RwLock<HashMap<String, usize>>>,
}

impl SessionAffinity {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the recorded backend index for `thread_id`, if any.
    pub async fn preferred(&self, thread_id: &str) -> Option<usize> {
        self.inner.read().await.get(thread_id).copied()
    }

    /// Associates `thread_id` with `backend_index` after a successful request.
    /// Called on every success so the map always reflects the most recent
    /// backend used — including after a fallback re-route.
    pub async fn record(&self, thread_id: &str, backend_index: usize) {
        self.inner
            .write()
            .await
            .insert(thread_id.to_string(), backend_index);
    }
}

impl Default for SessionAffinity {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_preference_by_default() {
        let a = SessionAffinity::new();
        assert_eq!(a.preferred("thread-1").await, None);
    }

    #[tokio::test]
    async fn records_and_retrieves_preference() {
        let a = SessionAffinity::new();
        a.record("thread-1", 2).await;
        assert_eq!(a.preferred("thread-1").await, Some(2));
    }

    #[tokio::test]
    async fn different_threads_are_independent() {
        let a = SessionAffinity::new();
        a.record("thread-a", 0).await;
        a.record("thread-b", 1).await;
        assert_eq!(a.preferred("thread-a").await, Some(0));
        assert_eq!(a.preferred("thread-b").await, Some(1));
    }

    #[tokio::test]
    async fn record_updates_existing_preference() {
        let a = SessionAffinity::new();
        a.record("thread-1", 0).await;
        a.record("thread-1", 1).await;
        assert_eq!(a.preferred("thread-1").await, Some(1));
    }

    #[tokio::test]
    async fn clone_shares_map_not_copies() {
        let original = SessionAffinity::new();
        let cloned = original.clone();
        original.record("thread-1", 3).await;
        assert_eq!(
            cloned.preferred("thread-1").await,
            Some(3),
            "clone must share the Arc — changes must be visible across handles"
        );
    }
}
