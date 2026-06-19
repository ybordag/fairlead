use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum CircuitState {
    /// Healthy: all requests pass through.
    Closed,
    /// Broken: requests are rejected immediately without touching the backend.
    Open { since: Instant },
    /// Recovery probe: one request gets through.
    /// Success → Closed. Failure → Open again.
    HalfOpen,
}

/// Per-backend circuit breaker held behind `Arc<RwLock<CircuitBreaker>>` in `BackendState`.
///
/// Transitions:
///   Closed  ──(failure_threshold consecutive failures)──► Open
///   Open    ──(cooldown elapsed)──────────────────────► HalfOpen
///   HalfOpen──(success)────────────────────────────────► Closed
///   HalfOpen──(failure)────────────────────────────────► Open
#[derive(Debug)]
pub struct CircuitBreaker {
    state: CircuitState,
    consecutive_failures: u32,
    failure_threshold: u32,
    cooldown: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            failure_threshold,
            cooldown,
        }
    }

    pub fn state(&self) -> &CircuitState {
        &self.state
    }

    /// Returns `true` if a request should be forwarded to this backend.
    /// Transitions `Open → HalfOpen` when the cooldown has elapsed.
    pub fn is_available(&mut self) -> bool {
        // Copy the open-since instant out so we can mutate self.state after.
        let open_since = match &self.state {
            CircuitState::Open { since } => Some(*since),
            CircuitState::Closed | CircuitState::HalfOpen => None,
        };

        match open_since {
            None => true,
            Some(since) if since.elapsed() < self.cooldown => false,
            Some(_) => {
                self.state = CircuitState::HalfOpen;
                true
            }
        }
    }

    /// Call on a successful response (2xx/3xx/4xx).
    /// Resets the failure counter and closes the circuit from any state.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
    }

    /// Call on a connection error or 5xx response.
    /// Opens the circuit after `failure_threshold` consecutive failures.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.failure_threshold {
            self.state = CircuitState::Open {
                since: Instant::now(),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cb(threshold: u32, cooldown_ms: u64) -> CircuitBreaker {
        CircuitBreaker::new(threshold, Duration::from_millis(cooldown_ms))
    }

    #[test]
    fn starts_closed_and_available() {
        let mut c = cb(3, 30_000);
        assert!(c.is_available());
        assert!(matches!(c.state(), CircuitState::Closed));
    }

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let mut c = cb(3, 30_000);
        c.record_failure();
        c.record_failure();
        assert!(c.is_available(), "still closed after 2 failures");
        c.record_failure();
        assert!(!c.is_available(), "should be open after 3 failures");
        assert!(matches!(c.state(), CircuitState::Open { .. }));
    }

    #[test]
    fn success_resets_failure_counter() {
        let mut c = cb(3, 30_000);
        c.record_failure();
        c.record_failure();
        c.record_success(); // resets counter
        c.record_failure();
        c.record_failure();
        assert!(c.is_available(), "counter was reset so should still be closed");
    }

    #[test]
    fn open_circuit_blocks_immediately() {
        let mut c = cb(1, 30_000);
        c.record_failure();
        assert!(!c.is_available());
        assert!(!c.is_available(), "still blocked on second check");
    }

    #[test]
    fn transitions_to_half_open_after_cooldown() {
        let mut c = cb(1, 10); // 10 ms cooldown
        c.record_failure();
        assert!(!c.is_available());

        std::thread::sleep(Duration::from_millis(20));

        assert!(c.is_available(), "should be half-open after cooldown");
        assert!(matches!(c.state(), CircuitState::HalfOpen));
    }

    #[test]
    fn success_in_half_open_closes_circuit() {
        let mut c = cb(1, 10);
        c.record_failure();
        std::thread::sleep(Duration::from_millis(20));
        assert!(c.is_available()); // transitions to HalfOpen
        c.record_success();
        assert!(matches!(c.state(), CircuitState::Closed));
        assert!(c.is_available());
    }

    #[test]
    fn failure_in_half_open_reopens_circuit() {
        let mut c = cb(1, 10);
        c.record_failure();
        std::thread::sleep(Duration::from_millis(20));
        assert!(c.is_available()); // transitions to HalfOpen
        c.record_failure();
        assert!(!c.is_available(), "should be open again after failure in half-open");
        assert!(matches!(c.state(), CircuitState::Open { .. }));
    }
}
