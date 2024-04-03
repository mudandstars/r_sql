use crate::{input_parser::QueryIterator, query::Statement};

use super::StatementParser;

pub struct SelectStatementParser();

impl StatementParser for SelectStatementParser {
    fn parse_statement(&mut self, query_iterator: &mut QueryIterator) -> Statement {
        let mut selection: Vec<String> = Vec::new();
        let mut current_word = "";

        loop {
            let current_slice_vector: Vec<&str> = query_iterator
                .next()
                .expect("Invalid query.")
                .split(',')
                .collect();

            for word in current_slice_vector {
                if !word.is_empty() {
                    current_word = word;

                    if word != "FROM" {
                        selection.push(String::from(word));
                    }
                }
            }

            if current_word.to_uppercase() == "FROM" {
                break;
            }
        }

        let table_name = query_iterator.next().expect("Invalid query.").replace(';', "");

        Statement::Select { selection, table_name }
    }
}
