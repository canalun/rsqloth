use rsqloth_core::echo;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    for arg in args {
        echo(&arg)
    }
}
