extern crate hybrid_clocks as hlc;

use crate::hlc::Clock;

fn main() {
    let mut clock_a = Clock::wall_ns();
    let mut clock_b = Clock::wall_ns();

    let b0 = clock_b.now();
    let a0 = clock_a.now();
    let () = clock_a.observe(&b0).expect("observing");
    let a1 = clock_a.now();

    println!("b0: {:?} / {}", b0, b0);
    println!("a0: {:?} / {}", a0, a0);
    println!("recv {} -> {}", b0, a1);
}
