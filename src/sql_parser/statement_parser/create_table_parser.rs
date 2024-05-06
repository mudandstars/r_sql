use crate::sql_parser::query::Statement;

use super::StatementParser;

const CREATE_TABLE_GRAPHEMS: [&str; 2] = ["CREATE", "TABLE"];

pub struct CreateTableStatementParser {
    state: ParserState,
}

impl StatementParser for CreateTableStatementParser {
    fn parse_statement(&mut self, graphemes: Vec<String>) -> super::StatementResult {
        let mut table_name = String::new();
        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut current_column: Vec<String> = Vec::new();

        for grapheme in graphemes {
            let changed_parser_state = self.change_parser_state(&grapheme);

            if changed_parser_state {
                continue;
            }

            match self.state {
                ParserState::TableName => {
                    if grapheme.to_uppercase() != CREATE_TABLE_GRAPHEMS[0]
                        && grapheme.to_uppercase() != CREATE_TABLE_GRAPHEMS[1]
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

                    if grapheme.to_uppercase() == "PRIMARY" {
                        current_column.push(String::from("PRIMARY KEY"));
                        continue;
                    }

                    if grapheme.to_uppercase() == "KEY" {
                        continue;
                    }

                    current_column.push(grapheme.to_string());
                }
            }
        }

        Ok(Statement::CreateTable {
            table_name,
            columns,
        })
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

#[cfg(test)]
mod tests {
    use crate::sql_parser::SqlParser;

    #[test]
    fn test_can_parse_a_create_table_statement() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from(
            "CREATE TABLE users(
                id PRIMARY KEY,
                name VARCHAR,
                email VARCHAR
            );",
        ));

        assert_eq!(
            query.unwrap().statement.to_string(),
            String::from("CREATE TABLE users(\nid PRIMARY KEY,\nname VARCHAR,\nemail VARCHAR\n);")
        );
    }

    #[test]
    fn test_can_parse_a_create_table_statement_without_unnecessary_whitespace() {
        let input_parser = SqlParser();
        let query = input_parser.parse_query(String::from(
            "CREATE TABLE users(id PRIMARY KEY,name VARCHAR, email VARCHAR);",
        ));

        assert_eq!(
            query.unwrap().statement.to_string(),
            String::from("CREATE TABLE users(\nid PRIMARY KEY,\nname VARCHAR,\nemail VARCHAR\n);")
        );
    }
}
