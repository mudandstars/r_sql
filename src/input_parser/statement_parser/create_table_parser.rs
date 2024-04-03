use crate::query::Statement;

use super::StatementParser;

const VARCHAR_GRAPHEME: &str = "VARCHAR";
const CREATE_TABLE_GRAPHEMS: [&str; 2] = ["CREATE", "TABLE"];
const AVAILABLE_DATA_TYPES: [&str; 3] = [VARCHAR_GRAPHEME, "PRIMARY", "KEY"];

pub struct CreateTableStatementParser {
    state: ParserState,
}

impl StatementParser for CreateTableStatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> Statement {
        let mut table_name = String::new();
        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut current_column: Vec<String> = Vec::new();
        let mut previous_grapheme: &str;

        for grapheme in graphemes {
            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::TableName => {
                    if grapheme != CREATE_TABLE_GRAPHEMS[0] && grapheme != CREATE_TABLE_GRAPHEMS[1]
                    {
                        table_name = grapheme.to_string();
                        self.state = ParserState::Columns
                    }
                }
                ParserState::Columns => {
                    if grapheme == "(" || grapheme == ")" {
                        continue;
                    }

                    if grapheme == "," || grapheme == ";" {
                        columns.push(current_column);
                        current_column = Vec::new();
                        continue;
                    }

                    current_column.push(grapheme.to_string());
                }
            }
        }

        Statement::CreateTable {
            table_name,
            columns,
        }
    }
}

impl CreateTableStatementParser {
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
            ParserState::Columns => false,
        }
    }
}

enum ParserState {
    TableName,
    Columns,
}
