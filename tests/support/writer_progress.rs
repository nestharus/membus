//! A monotonic watchdog for concurrent write encounters, independent of total I/O duration.
use std::time::Duration;

#[derive(Debug)]
pub struct WriterProgress {
    completed: Vec<usize>,
    last: Vec<Duration>,
    goal: usize,
    limit: Duration,
}
impl WriterProgress {
    pub fn new(writers: usize, goal: usize, limit: Duration) -> Self {
        assert!(writers > 0 && goal > 0 && !limit.is_zero());
        Self { completed: vec![0; writers], last: vec![Duration::ZERO; writers],
               goal, limit }
    }
    pub fn record(&mut self, writer: usize, completed: usize, at: Duration) {
        assert!(writer < self.completed.len() && completed <= self.goal);
        if completed > self.completed[writer] {
            self.completed[writer] = completed;
            self.last[writer] = at;
        }
    }
    pub fn remaining(&self, now: Duration) -> Result<Option<Duration>, usize> {
        let mut shortest = None;
        for (writer, completed) in self.completed.iter().enumerate() {
            if *completed == self.goal { continue; }
            let left = self.limit.checked_sub(now.saturating_sub(self.last[writer]))
                .filter(|left| !left.is_zero()).ok_or(writer)?;
            shortest = Some(shortest.map_or(left, |old| std::cmp::min(old, left)));
        }
        Ok(shortest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn s(seconds: u64) -> Duration { Duration::from_secs(seconds) }

    #[test]
    fn healthy_progress_can_outlive_the_total_burst_budget() {
        let mut progress = WriterProgress::new(4, 8, s(60));
        for completed in 1..=8 {
            let now = s(completed as u64 * 10);
            for writer in 0..4 { progress.record(writer, completed, now); }
            assert!(progress.remaining(now).is_ok(), "healthy writers still advance: {progress:?}");
        }
        assert_eq!(progress.remaining(s(200)), Ok(None));
    }
    #[test]
    fn a_finished_peer_does_not_extend_a_stalled_writers_bound() {
        let mut progress = WriterProgress::new(2, 8, s(60));
        progress.record(1, 8, s(59));
        assert_eq!(progress.remaining(s(60)), Err(0));
    }
    #[test]
    fn duplicate_and_older_counts_do_not_renew_a_stall_deadline() {
        let mut progress = WriterProgress::new(1, 8, s(60));
        progress.record(0, 1, s(10));
        progress.record(0, 1, s(59));
        progress.record(0, 0, s(65));
        assert_eq!(progress.remaining(s(70)), Err(0));
    }
    #[test]
    fn every_completed_writer_remains_complete_without_more_events() {
        let mut progress = WriterProgress::new(2, 8, s(60));
        progress.record(0, 8, s(10));
        progress.record(1, 8, s(20));
        assert_eq!(progress.remaining(s(200)), Ok(None));
    }
    #[test]
    fn the_same_sixty_second_stall_boundary_is_enforced() {
        let mut progress = WriterProgress::new(1, 8, s(60));
        progress.record(0, 1, s(0));
        assert_eq!(progress.remaining(s(59)), Ok(Some(s(1))));
        assert_eq!(progress.remaining(s(60)), Err(0));
        progress.record(0, 8, s(60));
        assert_eq!(progress.remaining(s(60)), Ok(None));
    }
}
