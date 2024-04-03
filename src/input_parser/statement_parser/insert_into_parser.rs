use crate::{input_parser::QueryIterator, query::Statement};

use super::StatementParser;

pub struct InsertIntoParser {
    state: ParserState,
}

impl StatementParser for InsertIntoParser {
    fn parse_statement(&mut self, query_iterator: &mut QueryIterator) -> Statement {
        let values: Vec<&str> = query_iterator.collect();
        let mut cleaned_query: Vec<String> = Vec::with_capacity(values.len());

        for value in values {
            let value_with_spaces = value
                .replace('(', " ( ")
                .replace(',', " , ")
                .replace(')', " ) ")
                .replace(';', " ; ");

            for value in value_with_spaces.split(' ').collect::<Vec<&str>>() {
                let cleaned_value = value.replace(',', "").replace(')', "");
                let value = cleaned_value.trim();
                if !value.is_empty() {
                    cleaned_query.push(value.to_string());
                }
            }
        }

        let mut table_name = String::new();
        let mut columns: Vec<String> = Vec::new();
        let mut values: Vec<Vec<String>> = Vec::new();
        let mut current_values: Vec<String> = Vec::new();

        for value in cleaned_query {
            let changed_parser_state = self.change_parser_state(&value);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::TableName => table_name = value.to_string(),
                ParserState::Columns => columns.push(value.to_string()),
                ParserState::Values => {
                    if (&value == "(" || &value == ";") && !current_values.is_empty() {
                        values.push(current_values);
                        current_values = Vec::new();
                    } else if &value != "(" {
                        current_values.push(value.to_string())
                    }
                }
            }
        }

        Statement::InsertInto {
            table_name,
            columns,
            values,
        }
    }
}

impl InsertIntoParser {
    pub fn new() -> Self {
        Self {
            state: ParserState::TableName,
        }
    }

    fn change_parser_state(&mut self, value: &str) -> bool {
        match self.state {
            ParserState::TableName => {
                if value == "(" {
                    self.state = ParserState::Columns;
                    true
                } else {
                    false
                }
            }
            ParserState::Columns => {
                if value.to_uppercase() == "VALUES" {
                    self.state = ParserState::Values;
                    true
                } else {
                    false
                }
            }
            ParserState::Values => false,
        }
    }
}

enum ParserState {
    TableName,
    Columns,
    Values,
}
