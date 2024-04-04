use crate::query::Statement;

use super::StatementParser;

pub struct InsertIntoParser {
    state: ParserState,
}

impl StatementParser for InsertIntoParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> Statement {
        let mut table_name = String::new();
        let mut column_names: Vec<String> = Vec::new();
        let mut values: Vec<Vec<String>> = Vec::new();
        let mut current_values: Vec<String> = Vec::new();

        for grapheme in graphemes {
            if grapheme == "," || grapheme == ")" {
                continue;
            }

            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::TableName => table_name = grapheme,
                ParserState::Columns => column_names.push(grapheme),
                ParserState::Values => {
                    if (grapheme == "(" || grapheme == ";") && !current_values.is_empty() {
                        if current_values.len() != column_names.len() {
                            panic!("Invalid query. Your provided values must match the provided columns.")
                        }

                        values.push(current_values);
                        current_values = Vec::new();
                    } else if grapheme != "(" {
                        current_values.push(grapheme)
                    }
                }
            }
        }

        Statement::InsertInto {
            table_name,
            column_names,
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

    fn change_parser_state(&mut self, grapheme: &str) -> bool {
        match self.state {
            ParserState::TableName => {
                if grapheme == "(" {
                    self.state = ParserState::Columns;
                    true
                } else {
                    false
                }
            }
            ParserState::Columns => {
                if grapheme.to_uppercase() == "VALUES" {
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
