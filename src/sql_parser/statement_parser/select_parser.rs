use std::collections::HashMap;

use crate::sql_parser::query::Statement;

use super::StatementParser;

const SELECT_GRAPHEME: &str = "SELECT";
const FROM_GRAPHEME: &str = "FROM";
const WHERE_GRAPHEME: &str = "WHERE";
const AND_GRAPHEME: &str = "AND";

pub struct SelectStatementParser {
    state: ParserState,
}

impl StatementParser for SelectStatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> super::StatementResult {
        let mut selection: Vec<String> = Vec::new();
        let mut table_name = String::new();
        let mut last_where_attribute = String::new();
        let mut where_clauses: HashMap<String, String> = HashMap::new();

        for grapheme in graphemes {
            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::Selection => {
                    if grapheme.to_uppercase() != SELECT_GRAPHEME && grapheme != "," {
                        selection.push(grapheme);
                    }
                }
                ParserState::TableName => {
                    if grapheme != ";" {
                        table_name = grapheme;
                    }
                }
                ParserState::WhereClauses => {
                    if grapheme != "=" && grapheme != AND_GRAPHEME {
                        if last_where_attribute.is_empty() {
                            last_where_attribute = grapheme;
                        } else {
                            where_clauses.insert(last_where_attribute.to_string(), grapheme);
                            last_where_attribute = String::new();
                        }
                    }
                }
            }
        }

        Ok(Statement::Select {
            selection,
            table_name,
            where_clauses,
        })
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
            ParserState::TableName => {
                if grapheme.to_uppercase() == WHERE_GRAPHEME {
                    self.state = ParserState::WhereClauses;
                    true
                } else {
                    false
                }
            }
            ParserState::WhereClauses => false,
        }
    }
}

enum ParserState {
    TableName,
    Selection,
    WhereClauses,
}

#[cfg(test)]
mod tests {
    use crate::sql_parser::{query::Statement, SqlParser};

    #[test]
    fn test_can_create_a_parsed_input_from_a_simple_select_query() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from("SELECT * FROM users;"));

        assert_eq!(
            query.unwrap().statement.to_string(),
            String::from("SELECT * FROM users;")
        );
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_for_specific_columns() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from(
            "SELECT id,foreign_id, number,name,job, another FROM users;",
        ));

        assert_eq!(
            query.unwrap().statement.to_string(),
            String::from("SELECT id, foreign_id, number, name, job, another FROM users;")
        );
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_with_a_where_statement() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from("SELECT * FROM users WHERE id = 5;"));

        match query.unwrap().statement {
            Statement::Select {
                where_clauses,
                selection,
                ..
            } => {
                assert_eq!(where_clauses.get("id").unwrap(), "5");
                assert_eq!(selection.len(), 1);
                assert_eq!(selection.first().unwrap(), "*");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_with_multiple_where_statements() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from(
            "SELECT * FROM users WHERE ranking_number = 1 AND name = 'rust';",
        ));

        match query.unwrap().statement {
            Statement::Select { where_clauses, .. } => {
                assert_eq!(where_clauses.get("ranking_number").unwrap(), "1");
                assert_eq!(where_clauses.get("name").unwrap(), "'rust'");
            }
            _ => panic!(),
        }
    }
}
