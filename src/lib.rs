//! Implementation of Hybrid Logical Clocks.
//!
//! This is based on the paper "Logical Physical Clocks and Consistent
//! Snapshots in Globally Distributed Databases". Provides a
//! strictly-monotonic clock that can be used to determine if one event
//! `happens-before` another.

#![deny(warnings)]

#[cfg(feature = "serialization")]
extern crate serde;
#[cfg(feature = "serialization")]
#[macro_use]
extern crate serde_derive;
#[cfg(all(feature = "serialization", test))]
extern crate serde_json;

use std::cmp::Ordering;
use std::fmt;

use thiserror::Error;

mod source;
pub use crate::source::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Offset greater than limit")]
    OffsetTooGreat,
    #[error("Outside of specified offset")]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error("Integer conversion error")]
    FromInt(#[from] std::num::TryFromIntError),
    #[error("Outside supported time range: {0}ticks")]
    SupportedTime(u128),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A value that represents a logical timestamp.
///
/// These allow us to describe at least a partial ordering over events, in the
/// same style as Lamport Clocks. In summary, if `a < b` then we can say that `a` logically
/// `happens-before` `b`. Because they are scalar values, they can't be used to tell between whether:
///
///  * `a` happenned concurrently with `b`, or
///  * `a` is part of `b`'s causal history, or vica-versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp<T> {
    /// An epoch counter.
    pub epoch: u32,
    /// The Wall-clock time as returned by the clock source.
    pub time: T,
    /// A Lamport clock used to disambiguate events that are given the same
    /// Wall-clock time. This is reset whenever `time` is incremented.
    pub count: u32,
}

/// The main clock type.
#[derive(Debug, Clone)]
pub struct Clock<S: ClockSource> {
    src: S,
    epoch: u32,
    last_observed: Timestamp<S::Time>,
}

/// A wrapper around `Clock` that will refuse updates outside of our tolerance.
#[derive(Debug, Clone)]
pub struct OffsetLimiter<S: ClockSource> {
    clock: Clock<S>,
    max_offset: S::Delta,
}

impl Clock<WallNS> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn wall_ns() -> Result<Clock<WallNS>> {
        Clock::new(WallNS)
    }
}

impl Clock<WallMS> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn wall_ms() -> Result<Clock<WallMS>> {
        Clock::new(WallMS)
    }
}

impl Clock<ManualClock> {
    /// Returns a `Clock` that uses WallNS-clock time.
    pub fn manual(t: u64) -> Result<Clock<ManualClock>> {
        Clock::new(ManualClock::new(t))
    }
    pub fn set_time(&mut self, t: u64) {
        self.src.set_time(t)
    }
}

impl<S: ClockSource> Clock<S> {
    /// Creates a clock with `src` as the time provider.
    pub fn new(mut src: S) -> Result<Self> {
        let init = src.now()?;
        let clock = Clock {
            src: src,
            last_observed: Timestamp {
                epoch: 0,
                time: init,
                count: 0,
            },
            epoch: 0,
        };
        Ok(clock)
    }

    /// Creates a clock with `src` as the time provider, and `diff` as how far
    /// in the future we don't mind seeing updates from.
    pub fn with_max_diff(self, max_offset: S::Delta) -> OffsetLimiter<S> {
        OffsetLimiter::new(self, max_offset)
    }

    /// Used to create a new "epoch" of clock times, mostly useful as a manual
    /// override when a cluster member has skewed the clock time far
    /// into the future.
    pub fn set_epoch(&mut self, epoch: u32) {
        self.epoch = epoch;
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn now(&mut self) -> Result<Timestamp<S::Time>> {
        let pt = self.read_pt()?;
        self.do_observe(&pt);
        Ok(self.last_observed)
    }

    fn do_observe(&mut self, observation: &Timestamp<S::Time>) {
        let lp = self.last_observed.clone();

        self.last_observed = match (
            lp.epoch.cmp(&observation.epoch),
            lp.time.cmp(&observation.time),
            lp.count.cmp(&observation.count),
        ) {
            (Ordering::Less, _, _) | (Ordering::Equal, Ordering::Less, _) => observation.clone(),
            (Ordering::Equal, Ordering::Equal, Ordering::Less) => Timestamp {
                count: observation.count + 1,
                ..lp
            },
            _ => Timestamp {
                count: lp.count + 1,
                ..lp
            },
        };
    }

    /// Accepts a timestamp from an incoming message, and updates the clock
    /// so that further calls to `now` will always return a timestamp that
    /// `happens-after` either locally generated timestamps or that of the
    /// input message.
    pub fn observe(&mut self, msg: &Timestamp<S::Time>) {
        self.do_observe(&msg);
    }

    fn read_pt(&mut self) -> Result<Timestamp<S::Time>> {
        Ok(Timestamp {
            epoch: self.epoch,
            time: self.src.now()?,
            count: 0,
        })
    }
}
impl<S: ClockSource> OffsetLimiter<S> {
    pub fn new(clock: Clock<S>, max_offset: S::Delta) -> Self {
        OffsetLimiter { clock, max_offset }
    }
    /// Accepts a timestamp from an incoming message, and updates the clock
    /// so that further calls to `now` will always return a timestamp that
    /// `happens-after` either locally generated timestamps or that of the
    /// input message. Returns an Error iff the delta from our local lock to
    /// the observed timestamp is greater than our configured limit.
    pub fn observe(&mut self, msg: &Timestamp<S::Time>) -> Result<()> {
        let pt = self.clock.read_pt()?;
        self.verify_offset(&pt, msg)?;
        self.clock.observe(&msg);
        Ok(())
    }

    /// Creates a unique monotonic timestamp suitable for annotating messages we send.
    pub fn now(&mut self) -> Result<Timestamp<S::Time>> {
        self.clock.now()
    }

    fn verify_offset(&self, pt: &Timestamp<S::Time>, msg: &Timestamp<S::Time>) -> Result<()> {
        let diff = msg.time - pt.time;
        if diff > self.max_offset {
            return Err(Error::OffsetTooGreat);
        }

        Ok(())
    }

    /// Extract the inner `Clock`
    pub fn into_inner(self) -> Clock<S> {
        self.clock
    }

    /// Get a reference to the inner `Clock`
    pub fn inner(&self) -> &Clock<S> {
        &self.clock
    }

    /// Get a mutable reference to the inner `Clock`
    pub fn inner_mut(&mut self) -> &mut Clock<S> {
        &mut self.clock
    }
}

impl<T: fmt::Display> fmt::Display for Timestamp<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}:{}+{}", self.epoch, self.time, self.count)
    }
}

impl<T> Timestamp<T> {
    pub fn time_into<U: From<T>>(self) -> Timestamp<U> {
        Timestamp {
            epoch: self.epoch,
            time: self.time.into(),
            count: self.count,
        }
    }
}

#[cfg(feature = "serialization")]
mod serde_impl;

#[cfg(test)]
mod tests {
    use super::Timestamp;
    use suppositions::generators::*;

    pub fn timestamps<C: Generator + 'static>(
        times: C,
    ) -> Box<dyn GeneratorObject<Item = Timestamp<C::Item>>> {
        let epochs = u32s();
        let counts = u32s();
        (epochs, times, counts)
            .map(|(epoch, time, count)| Timestamp { epoch, time, count })
            .boxed()
    }
}
