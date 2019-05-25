use std::fmt;
use std::ops::Sub;

mod manual;
mod wall_ns;
pub use self::manual::*;
pub use self::wall_ns::*;

/// Describes the interface that the inner clock source must provide.
pub trait ClockSource {
    /// Represents the described clock time.
    type Time: Ord + Copy + Sub<Output = Self::Delta> + fmt::Debug;
    /// The difference between two timestamps.
    type Delta: Ord;
    /// Returns the current clock time.
    fn now(&mut self) -> Self::Time;
}
