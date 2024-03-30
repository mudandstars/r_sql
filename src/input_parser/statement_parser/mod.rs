mod create_table_parser;
mod select_parser;

use crate::input_parser::statement_parser::create_table_parser::CreateTableStatementParser;
use crate::input_parser::statement_parser::select_parser::SelectStatementParser;

use crate::query::{Statement, StatementType};

use super::QueryIterator;

pub trait StatementParser {
    fn parse_statement(&self, query_iterator: &mut QueryIterator) -> Statement;
}

pub fn statement_parser_factory(statement_type: StatementType) -> Box<dyn StatementParser> {
    match statement_type {
        StatementType::CreateTable => Box::new(CreateTableStatementParser()),
        StatementType::Select => Box::new(SelectStatementParser()),
        _ => panic!("Not implemented yet."),
    }
}
