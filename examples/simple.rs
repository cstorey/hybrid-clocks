extern crate hybrid_clocks as hlc;
extern crate time;
use hlc::Clock;
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let mut clock_a = Clock::wall();
    let mut clock_b = Clock::wall();

    let b0 = clock_b.now();
    let a0 = clock_a.now();
    let () = clock_a.observe(&b0).expect("observing");
    let a1 = clock_a.now();

    println!("b0: {:?} / {}", b0, b0);
    println!("a0: {:?} / {}", a0, a0);
    println!("recv {} -> {}", b0, a1);
}
