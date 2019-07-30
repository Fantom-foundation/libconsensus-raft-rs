use std::time::{Duration, Instant};

pub struct Ticker {
    last_instant: Instant,
    timeout: Duration,
    remaining: Duration,
}

impl Ticker {
    pub fn new(period: Duration) -> Self {
        Ticker {
            last_instant: Instant::now(),
            timeout: period,
            remaining: period,
        }
    }

    pub fn tick<T: FnMut()>(&mut self, mut callback: T) -> Duration {
        let elapsed_duration = self.last_instant.elapsed();

        if elapsed_duration >= self.remaining {
            callback();
            self.last_instant = Instant::now();
            self.remaining = self.timeout;
        } else {
            self.remaining -= elapsed_duration;
        }

        self.remaining
    }
}
