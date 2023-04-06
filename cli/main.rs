use rsqloth_core::format_insert_queries;
use std::{env, fs::OpenOptions, io::Read, process};

fn main() {
    let args: Vec<String> = env::args().collect();

    for path in args.iter().skip(1) {
        let mut f = OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap_or_else(|err| {
                eprintln!("failed to open the file. error: {err}");
                process::exit(1);
            });

        let mut sql = String::new();
        f.read_to_string(&mut sql).unwrap_or_else(|err| {
            eprintln!("failed to read the file. error: {err}");
            process::exit(1);
        });

        let res = format_insert_queries(&sql);
        match res {
            Ok(res) => {
                println!("{res}");
                process::exit(0);
            }
            Err(err) => {
                eprintln!("failed to format. error: {err}");
                process::exit(1);
            }
        }
    }
}
