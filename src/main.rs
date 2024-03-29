use r_sql::binary_writer::BinaryWriter;
use r_sql::input_parser::InputParser;
use std::env;

fn main() {
    let query = get_query_from_cli_args();
    println!("{}", query);

    let input_parser = InputParser::new(query);

    let binary_writer = BinaryWriter::new(input_parser);

    binary_writer.write();
}

fn get_query_from_cli_args() -> String {
    let mut args = env::args();

    args.next();
    args.next()
        .expect("Please specify one query wrapped in double quotes.")
}
