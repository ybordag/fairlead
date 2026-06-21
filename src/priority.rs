use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::Priority;

#[derive(Clone)]
pub struct PriorityLimiter {
    realtime: PriorityBucket,
    batch: PriorityBucket,
    background: PriorityBucket,
}

#[derive(Clone)]
struct PriorityBucket {
    limit: usize,
    semaphore: Arc<Semaphore>,
    in_flight: Arc<AtomicUsize>,
}

pub struct PriorityPermit {
    _permit: OwnedSemaphorePermit,
    in_flight: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrioritySnapshot {
    pub priority: Priority,
    pub limit: usize,
    pub in_flight: usize,
}

impl PriorityLimiter {
    pub fn new(realtime_limit: usize, batch_limit: usize, background_limit: usize) -> Self {
        Self {
            realtime: PriorityBucket::new(realtime_limit),
            batch: PriorityBucket::new(batch_limit),
            background: PriorityBucket::new(background_limit),
        }
    }

    pub fn try_acquire(&self, priority: Priority) -> Option<PriorityPermit> {
        self.bucket(priority).try_acquire()
    }

    pub fn snapshots(&self) -> Vec<PrioritySnapshot> {
        [
            (Priority::Realtime, &self.realtime),
            (Priority::Batch, &self.batch),
            (Priority::Background, &self.background),
        ]
        .into_iter()
        .map(|(priority, bucket)| PrioritySnapshot {
            priority,
            limit: bucket.limit,
            in_flight: bucket.in_flight.load(Ordering::SeqCst),
        })
        .collect()
    }

    fn bucket(&self, priority: Priority) -> &PriorityBucket {
        match priority {
            Priority::Realtime => &self.realtime,
            Priority::Batch => &self.batch,
            Priority::Background => &self.background,
        }
    }
}

impl Default for PriorityLimiter {
    fn default() -> Self {
        Self::new(8, 4, 2)
    }
}

impl PriorityBucket {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            semaphore: Arc::new(Semaphore::new(limit)),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn try_acquire(&self) -> Option<PriorityPermit> {
        let permit = self.semaphore.clone().try_acquire_owned().ok()?;
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        Some(PriorityPermit {
            _permit: permit,
            in_flight: self.in_flight.clone(),
        })
    }
}

impl Drop for PriorityPermit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_blocks_when_priority_bucket_full() {
        let limiter = PriorityLimiter::new(1, 1, 1);
        let first = limiter.try_acquire(Priority::Realtime);
        assert!(first.is_some());
        assert!(limiter.try_acquire(Priority::Realtime).is_none());
    }

    #[test]
    fn dropping_permit_releases_capacity() {
        let limiter = PriorityLimiter::new(1, 1, 1);
        let first = limiter.try_acquire(Priority::Batch);
        assert!(first.is_some());
        assert!(limiter.try_acquire(Priority::Batch).is_none());
        drop(first);
        assert!(limiter.try_acquire(Priority::Batch).is_some());
    }

    #[test]
    fn priority_buckets_are_independent() {
        let limiter = PriorityLimiter::new(1, 1, 1);
        let realtime = limiter.try_acquire(Priority::Realtime);
        assert!(realtime.is_some());
        assert!(limiter.try_acquire(Priority::Batch).is_some());
        assert!(limiter.try_acquire(Priority::Background).is_some());
    }
}
