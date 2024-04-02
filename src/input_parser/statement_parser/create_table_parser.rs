use crate::{input_parser::QueryIterator, query::Statement};

use super::StatementParser;

pub struct CreateTableStatementParser();

impl StatementParser for CreateTableStatementParser {
    fn parse_statement(&self, query_iterator: &mut QueryIterator) -> Statement {
        let mut split_table_name_iterator =
            query_iterator.next().expect("Invalid query.").split('(');

        let table_name = split_table_name_iterator
            .next()
            .expect("Invalid query.")
            .to_string();

        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut current_column: Vec<String> = Vec::new();
        let mut current_word = "";

        if split_table_name_iterator.clone().next().is_some()
            && !split_table_name_iterator.clone().next().unwrap().is_empty()
        {
            current_column.push(split_table_name_iterator.next().unwrap().to_string());
        }

        loop {
            if self.is_last_word(current_word) {
                self.push_last_word_onto_column(current_word.to_string(), &mut current_column);

                columns.push(current_column);
                break;
            }

            self.handle_remaining_column_cases(
                current_word.to_string(),
                &mut current_column,
                &mut columns,
            );

            current_word = query_iterator.next().expect("Invalid query.");
        }

        Statement::CreateTable {
            columns,
            table_name,
        }
    }
}

impl CreateTableStatementParser {
    fn handle_remaining_column_cases(
        &self,
        word: String,
        column: &mut Vec<String>,
        columns: &mut Vec<Vec<String>>,
    ) {
        if self.column_ends_here(&word) {
            self.push_word_onto_column(word.to_string(), column);

            columns.push(column.to_vec());
            *column = Vec::new();
        } else if word.contains(',') {
            let mut subwords = word.split(',');

            self.push_word_onto_column(subwords.next().unwrap().to_string(), column);
            columns.push(column.to_vec());
            *column = Vec::new();

            self.push_word_onto_column(subwords.next().unwrap().to_string(), column);
        } else {
            self.push_word_onto_column(word.to_string(), column);
        }
    }

    fn push_word_onto_column(&self, word: String, column: &mut Vec<String>) {
        if !word.is_empty() && !word.ends_with(");") && word != "," {
            column.push(word.replace(',', ""));
        }
    }

    fn push_last_word_onto_column(&self, word: String, column: &mut Vec<String>) {
        if !word.is_empty() && word != ");" {
            column.push(word.replace(',', "").replace(");", ""));
        }
    }

    fn is_last_word(&self, word: &str) -> bool {
        word.ends_with(");")
    }

    fn column_ends_here(&self, word: &str) -> bool {
        word.ends_with(',')
    }
}
