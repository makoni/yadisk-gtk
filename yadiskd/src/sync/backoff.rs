#![allow(dead_code)]

use rand::Rng;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    jitter: bool,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration, jitter: bool) -> Self {
        Self { base, max, jitter }
    }

    pub fn delay(&self, attempt: u32) -> Duration {
        let mut rng = rand::thread_rng();
        self.delay_with_rng(attempt, &mut rng)
    }

    pub fn delay_with_rng<R: Rng + ?Sized>(&self, attempt: u32, rng: &mut R) -> Duration {
        let base_ms = self.base.as_millis().min(u128::from(u64::MAX)) as u64;
        let max_ms = self.max.as_millis().min(u128::from(u64::MAX)) as u64;
        let shift = attempt.min(16);
        let exp = base_ms.saturating_mul(1u64 << shift).min(max_ms);
        let delay_ms = if self.jitter {
            rng.gen_range(0..=exp)
        } else {
            exp
        };
        Duration::from_millis(delay_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn backoff_without_jitter_is_exponential() {
        let backoff = Backoff::new(
            Duration::from_millis(100),
            Duration::from_millis(800),
            false,
        );
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(
            backoff.delay_with_rng(0, &mut rng),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff.delay_with_rng(1, &mut rng),
            Duration::from_millis(200)
        );
        assert_eq!(
            backoff.delay_with_rng(2, &mut rng),
            Duration::from_millis(400)
        );
        assert_eq!(
            backoff.delay_with_rng(3, &mut rng),
            Duration::from_millis(800)
        );
        assert_eq!(
            backoff.delay_with_rng(4, &mut rng),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn backoff_with_jitter_is_capped() {
        let backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(800), true);
        let mut rng = StdRng::seed_from_u64(42);
        let delay = backoff.delay_with_rng(3, &mut rng);
        assert!(delay <= Duration::from_millis(800));
    }
}
