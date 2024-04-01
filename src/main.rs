use r_sql::io;
use r_sql::SQLEngine;
use std::env;
use std::io::{stdin, stdout, Write};

const QUIT_STRING: char = 'q';

fn main() {
    let engine = SQLEngine::new(io::Type::Binary);

    let mut args = env::args();

    if args.len() == 2 {
        run_engine_with_cli_arg(engine, &mut args);
    } else {
        run_engine_with_user_input(engine);
    }
}

fn run_engine_with_cli_arg(engine: SQLEngine, args: &mut std::env::Args) {
    let query = args
        .nth(1)
        .expect("Please specify one query wrapped in double quotes.");

    engine.execute(query);
}

fn run_engine_with_user_input(engine: SQLEngine) {
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
