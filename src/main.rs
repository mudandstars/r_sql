use r_sql::engine;
use r_sql::SQLEngine;
use std::env;
use std::io::{stdin, stdout, Write};

const QUIT_STRING: char = 'q';

fn main() {
    let engine = SQLEngine::new(engine::Type::Binary);

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

    match engine.execute(query) {
        Ok(..) => println!("Success"),
        Err(message) => println!("ERROR: {}", message),
    }
}

fn run_engine_with_user_input(engine: SQLEngine) {
    println!("Starting r_sql engine..");
    println!("Please type your query:");

    loop {
        print!("r_sql> ");
        stdout().flush().expect("Failed to flush stdout");

        let mut user_query = String::new();
        stdin()
            .read_line(&mut user_query)
            .expect("Failed to read line");
        user_query = user_query.strip_suffix('\n').unwrap().to_string();

        if user_query.len() == 1 && user_query.ends_with(QUIT_STRING) {
            println!("Quitting r_sql..");
            break;
        }

        match engine.execute(user_query) {
            Ok(..) => println!("Success"),
            Err(message) => println!("ERROR: {}", message),
        }
    }
}
