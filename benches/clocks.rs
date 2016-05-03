#![feature(test)]
extern crate test;
extern crate hybrid_clocks;
extern crate byteorder;

use std::io::Cursor;
use std::default::Default;
use hybrid_clocks::{Clock};

#[bench]
fn sending( b: &mut test::Bencher )  { 
    let mut clock = Clock::wall();
    b.iter(|| {
        clock.on_send()
    })
}

#[bench]
fn recv( b: &mut test::Bencher )  { 
    let mut clock = Clock::wall();
    let mut prev = clock.on_send();
    b.iter(|| {
        prev = clock.on_recv(&prev)
    })
}
