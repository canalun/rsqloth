use rsqloth_core::format_insert_queries;
use std::{
    env,
    fs::File,
    io::{Read, Write},
};

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut f = File::open(&args[1]).expect("aaaaaa");
    let mut sql = String::new();
    f.read_to_string(&mut sql).expect("aaaaa");

    let res = format_insert_queries(&sql);
    match res {
        Ok(res) => {
            let output_file = if args.len() > 2 { &args[2] } else { &args[1] };
            let mut f = File::create(output_file).expect("aaaaa");
            write!(f, "{}", res).expect("failed to write");
        }
        Err(_) => println!("NG"),
    }
}
