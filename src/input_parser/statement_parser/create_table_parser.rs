use crate::{input_parser::QueryIterator, query::Statement};

use super::StatementParser;

pub struct CreateTableStatementParser();

impl StatementParser for CreateTableStatementParser {
    fn parse_statement(&self, query_iterator: &mut QueryIterator) -> Statement {
        let table_name = query_iterator
            .next()
            .expect("Invalid query.")
            .split('(')
            .next()
            .expect("Invalid query.")
            .to_string();

        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut current_column: Vec<String> = Vec::new();
        let mut current_word = "";

        loop {
            if current_word == ");" {
                columns.push(current_column);
                break;
            }

            if current_word.ends_with(',') {
                columns.push(current_column);
                current_column = Vec::new();
            }

            current_word = query_iterator.next().expect("Invalid query.");

            if !current_word.is_empty() && current_word != ");" {
                current_column.push(current_word.replace(',', ""));
            }
        }

        Statement::CreateTable {
            columns,
            table_name,
        }
    }
}
