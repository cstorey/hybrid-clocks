use std::{cell::Cell, fmt};

use super::ClockSource;
use crate::Result;

pub struct ManualClock(Cell<u64>);
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
#[cfg_attr(feature = "serialization", derive(Serialize, Deserialize))]
pub struct ManualT(u64);

impl<'a> ClockSource for ManualClock {
    type Time = ManualT;
    type Delta = u64;
    fn now(&mut self) -> Result<Self::Time> {
        Ok(self.0.get().into())
    }
}

impl ManualClock {
    pub fn new(t: u64) -> ManualClock {
        ManualClock(Cell::new(t))
    }
    pub fn set_time(&self, t: u64) {
        self.0.set(t)
    }
}

impl From<u64> for ManualT {
    fn from(src: u64) -> Self {
        ManualT(src)
    }
}

impl std::ops::Sub for ManualT {
    type Output = u64;
    fn sub(self, other: Self) -> Self::Output {
        self.0 - other.0
    }
}

impl fmt::Display for ManualT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.0)
    }
}
