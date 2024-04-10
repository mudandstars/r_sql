mod create_table_parser;
mod insert_into_parser;
mod select_parser;

use crate::sql_parser::statement_parser::create_table_parser::CreateTableStatementParser;
use crate::sql_parser::statement_parser::insert_into_parser::InsertIntoParser;
use crate::sql_parser::statement_parser::select_parser::SelectStatementParser;

use crate::query::{Statement, StatementType};

pub trait StatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> Statement;
}

pub fn statement_parser_factory(statement_type: StatementType) -> Box<dyn StatementParser> {
    match statement_type {
        StatementType::CreateTable => Box::new(CreateTableStatementParser::new()),
        StatementType::Select => Box::new(SelectStatementParser::new()),
        StatementType::InsertInto => Box::new(InsertIntoParser::new()),
        _ => panic!("Not implemented yet."),
    }
}
