use r_sql::io;
use r_sql::SQLEngine;
use std::env;

fn main() {
    let engine = SQLEngine::new(io::Type::Binary);

    let query = get_query_from_cli_args();
    println!("{}", query);

    engine.execute(query);
}

fn get_query_from_cli_args() -> String {
    let mut args = env::args();

    args.next();
    args.next()
        .expect("Please specify one query wrapped in double quotes.")
}
