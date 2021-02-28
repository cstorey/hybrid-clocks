use std::fmt;
use std::ops::Sub;

mod wall_ms;
pub use self::wall_ms::*;
mod manual;
mod wall_ns;
pub use self::manual::*;
pub use self::wall_ns::*;
use crate::Result;

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Describes the interface that the inner clock source must provide.
pub trait ClockSource {
    /// Represents the described clock time.
    type Time: Ord + Copy + Sub<Output = Self::Delta> + fmt::Debug;
    /// The difference between two timestamps.
    type Delta: Ord;

    // TODO: We may need to generalise this.

    /// Returns the current clock time.
    fn now(&mut self) -> Result<Self::Time>;
}
