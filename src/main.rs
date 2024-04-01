use r_sql::io;
use r_sql::SQLEngine;
use std::io::{stdin, stdout, Write};

const QUIT_STRING: char = 'q';

fn main() {
    let engine = SQLEngine::new(io::Type::Binary);

    println!("Starting r_sql engine..");
    println!("Please type your query:");

    loop {
        print!("r_sql> ");
        stdout().flush().expect("Failed to flush stdout");

        let mut query = String::new();
        stdin().read_line(&mut query).expect("Failed to read line");
        query = query.strip_suffix('\n').unwrap().to_string();

        if query.len() == 1 && query.ends_with(QUIT_STRING) {
            println!("Quitting r_sql..");
            break;
        }

        engine.execute(query.clone());
    }
}
