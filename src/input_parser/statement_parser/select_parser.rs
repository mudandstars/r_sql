use crate::query::Statement;

use super::StatementParser;

const SELECT_GRAPHEME: &str = "SELECT";
const FROM_GRAPHEME: &str = "FROM";

pub struct SelectStatementParser {
    state: ParserState,
}

impl StatementParser for SelectStatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> Statement {
        let mut selection: Vec<String> = Vec::new();
        let mut table_name = String::new();

        for grapheme in graphemes {
            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::Selection => {
                    if grapheme != SELECT_GRAPHEME && grapheme != "," {
                        selection.push(grapheme);
                    }
                }
                ParserState::TableName => {
                    if grapheme != ";" {
                        table_name = grapheme;
                    }
                }
            }
        }

        Statement::Select {
            selection,
            table_name,
        }
    }
}

impl SelectStatementParser {
    pub fn new() -> Self {
        Self {
            state: ParserState::Selection,
        }
    }

    fn change_parser_state(&mut self, grapheme: &str) -> bool {
        match self.state {
            ParserState::Selection => {
                if grapheme.to_uppercase() == FROM_GRAPHEME {
                    self.state = ParserState::TableName;
                    true
                } else {
                    false
                }
            }
            ParserState::TableName => false,
        }
    }
}

enum ParserState {
    TableName,
    Selection,
}

#[cfg(test)]
mod tests {
    use crate::input_parser::InputParser;

    #[test]
    fn test_can_create_a_parsed_input_from_a_simple_select_query() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from("SELECT * FROM users;"));

        assert_eq!(
            query.statement.to_string(),
            String::from("SELECT * FROM users;")
        );
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_for_specific_columns() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from(
            "SELECT id,foreign_id, number,name,job, another FROM users;",
        ));

        assert_eq!(
            query.statement.to_string(),
            String::from("SELECT id, foreign_id, number, name, job, another FROM users;")
        );
    }
}
