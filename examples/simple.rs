extern crate hlc;
use hlc::Clock;
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let mut clock_a = Clock::wall();
    let mut clock_b = Clock::wall();

    let b0 = clock_b.on_send();
    let a0 = clock_a.on_send();
    let a1 = clock_a.on_recv(&b0);

    println!("a0: {:?}", a0);
    println!("b0: {:?}", b0);
    println!("recv {:?} -> {:?}", b0, a1);
}
