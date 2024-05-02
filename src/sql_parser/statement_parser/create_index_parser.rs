use crate::sql_parser::query::Statement;

use super::StatementParser;

const CREATE_INDEX_GRAPHEMS: [&str; 2] = ["CREATE", "INDEX"];

pub struct CreateIndexStatementParser {
    state: ParserState,
}

impl StatementParser for CreateIndexStatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> super::StatementResult {
        let mut table_name = String::new();
        let mut column_name = String::new();
        let mut index_name = String::new();

        dbg!(&graphemes);

        for grapheme in graphemes {
            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::Index => {
                    if grapheme.to_uppercase() != CREATE_INDEX_GRAPHEMS[0]
                        && grapheme.to_uppercase() != CREATE_INDEX_GRAPHEMS[1]
                    {
                        index_name = grapheme.to_string();
                    }
                }
                ParserState::Table => {
                    table_name = grapheme.to_string();
                    self.state = ParserState::Column
                }
                ParserState::Column => {
                    if grapheme != "(" && grapheme != ")" && grapheme != ";" {
                        column_name = grapheme.to_string();
                    }
                }
            }
        }

        Ok(Statement::CreateIndex {
            table_name,
            column_name,
            index_name,
        })
    }
}

impl CreateIndexStatementParser {
    pub fn new() -> Self {
        Self {
            state: ParserState::Index,
        }
    }

    fn change_parser_state(&mut self, grapheme: &str) -> bool {
        match self.state {
            ParserState::Index => {
                if grapheme.to_uppercase() == "ON" {
                    self.state = ParserState::Table;
                    true
                } else {
                    false
                }
            }
            ParserState::Table => {
                if grapheme == "(" {
                    self.state = ParserState::Column;
                    true
                } else {
                    false
                }
            }
            ParserState::Column => false,
        }
    }
}

enum ParserState {
    Index,
    Table,
    Column,
}

#[cfg(test)]
mod tests {
    use crate::sql_parser::SqlParser;

    #[test]
    fn test_can_parse_a_create_index_statement() {
        let input_parser = SqlParser();
        let query =
            input_parser.parse_query(String::from("CREATE INDEX my_index ON users(email);"));

        assert_eq!(
            query.unwrap().statement.to_string(),
            String::from("CREATE INDEX my_index\nON users(email);")
        );
    }
}
