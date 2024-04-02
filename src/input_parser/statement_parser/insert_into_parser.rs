use crate::{input_parser::QueryIterator, query::Statement};

use super::StatementParser;

pub struct InsertIntoParser();

impl StatementParser for InsertIntoParser {
    //   "INSERT INTO users(name,email, number) VALUES ('felix', 'felix@gmail.de', 12345), ('paul', 'paul@mail.com', 67890);",
    fn parse_statement(&self, query_iterator: &mut QueryIterator) -> Statement {
        let mut split_table_name_iterator =
            query_iterator.next().expect("Invalid query.").split('(');

        let table_name = split_table_name_iterator
            .next()
            .expect("Invalid query.")
            .to_string();

        let mut columns: Vec<String> = Vec::new();
        let mut values: Vec<Vec<String>> = Vec::new();
        let mut current_word = "";
        let mut current_values: Vec<String> = Vec::new();


//TODO ich kriege die columns schon, aber er switcht noch nicht richtig auf values und processed das dann noch nicht..

        if split_table_name_iterator.clone().next().is_some()
            && !split_table_name_iterator.clone().next().unwrap().is_empty()
        {
            for subword in split_table_name_iterator
                .next()
                .unwrap()
                .to_string()
                .split(',')
            {
                self.push_word_onto_correct_vector(
                    subword.to_string(),
                    &mut columns,
                    &mut current_values,
                );
            }
        }

        loop {
            if self.is_last_word_in_parentheses(current_word) {
                dbg!(&current_word);
                self.push_word_onto_correct_vector(
                    current_word.to_string(),
                    &mut columns,
                    &mut current_values,
                );

                if !current_values.is_empty() {
                    values.push(current_values);
                    break;
                }
            } else {
                self.handle_remaining_cases(
                    current_word.to_string(),
                    &mut columns,
                    &mut current_values,
                );
            }

            dbg!(&current_values, &columns);
            current_word = query_iterator.next().expect("Invalid query.");
        }

        Statement::InsertInto {
            table_name,
            columns,
            values,
        }
    }
}

impl InsertIntoParser {
    fn handle_remaining_cases(
        &self,
        word: String,
        columns: &mut Vec<String>,
        values: &mut Vec<String>,
    ) {
        dbg!(&word);
        if word.contains(',') {
            for subword in word.split(',') {
                self.push_word_onto_correct_vector(subword.to_string(), columns, values);
            }
        } else {
            self.push_word_onto_correct_vector(word, columns, values);
        }
    }

    fn push_word_onto_correct_vector(
        &self,
        word: String,
        columns: &mut Vec<String>,
        values: &mut Vec<String>,
    ) {
        if values.is_empty() && !self.word_should_be_escaped(&word) {
            columns.push(word.replace(',', "").replace(");", "").replace(')', ""));
        } else if !self.word_should_be_escaped(&word) {
            values.push(word.replace(',', "").replace(");", "").replace(')', ""));
        }
    }

    fn word_should_be_escaped(&self, word: &str) -> bool {
        word.is_empty() || word.ends_with(");") || word == ","
    }

    fn is_last_word_in_parentheses(&self, word: &str) -> bool {
        word.ends_with(')') || word.ends_with("),") || word.ends_with(");")
    }
}
