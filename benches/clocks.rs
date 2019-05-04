#![feature(test)]
extern crate byteorder;
extern crate hybrid_clocks;
extern crate test;

use hybrid_clocks::Clock;
use std::default::Default;
use std::io::Cursor;

#[bench]
fn sending(b: &mut test::Bencher) {
    let mut clock = Clock::wall();
    b.iter(|| clock.on_send())
}

#[bench]
fn recv(b: &mut test::Bencher) {
    let mut clock = Clock::wall();
    let mut prev = clock.on_send();
    b.iter(|| prev = clock.on_recv(&prev))
}
