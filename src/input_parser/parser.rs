use crate::query::Query;

use crate::query::StatementType;

use super::statement_parser::statement_parser_factory;

pub struct InputParser();

impl InputParser {
    pub fn parse_query(&self, input: String) -> Query {
        let trimmed_input = input.trim();
        let raw_graphemes: Vec<&str> = trimmed_input.split(' ').collect();
        let mut graphemes: Vec<String> = Vec::with_capacity(raw_graphemes.len());

        for grapheme in raw_graphemes {
            let grapheme_with_spaces = grapheme
                .replace('(', " ( ")
                .replace(',', " , ")
                .replace(')', " ) ")
                .replace(';', " ; ");

            for grapheme_with_space in grapheme_with_spaces.split(' ').collect::<Vec<&str>>() {
                let trimmed_subgrapheme = grapheme_with_space.trim();

                if !trimmed_subgrapheme.is_empty() {
                    graphemes.push(trimmed_subgrapheme.to_string());
                }
            }
        }

        if graphemes.last().expect("Query is required.") != ";" {
            panic!("Your statement must end with a semicolon.")
        } else if graphemes.len() < 2 {
            panic!("Invalid query.")
        }

        let statement_type = StatementType::new(&graphemes[0], &graphemes[1]);

        let mut statement_parser = statement_parser_factory(statement_type);

        Query::new(input, statement_parser.parse_statement(graphemes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_can_parse_a_create_table_statement() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from(
            "CREATE TABLE users(
                id PRIMARY KEY,
                name VARCHAR,
                email VARCHAR
            );",
        ));

        assert_eq!(
            query.statement.to_string(),
            String::from("CREATE TABLE users(\nid PRIMARY KEY,\nname VARCHAR,\nemail VARCHAR\n);")
        );
    }

    #[test]
    fn test_can_parse_a_create_table_statement_without_unnecessary_whitespace() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from(
            "CREATE TABLE users(id PRIMARY KEY,name VARCHAR, email VARCHAR);",
        ));

        assert_eq!(
            query.statement.to_string(),
            String::from("CREATE TABLE users(\nid PRIMARY KEY,\nname VARCHAR,\nemail VARCHAR\n);")
        );
    }

    #[test]
    fn test_can_parse_an_insert_statement() {
        let input_parser = InputParser();

        let query = input_parser.parse_query(String::from(
            "INSERT INTO users(name,email, number) VALUES ('felix', 'felix@gmail.de', 12345), ('paul', 'paul@mail.com', 67890);",
        ));
        assert_eq!(
            query.statement.to_string(),
            String::from("INSERT INTO users(\nname, email, number\n) VALUES (\n'felix', 'felix@gmail.de', 12345\n), (\n'paul', 'paul@mail.com', 67890\n);")
        );
    }

    #[test]
    #[should_panic]
    fn test_throws_for_insert_statement_where_some_values_tuple_length_does_not_match_columns_length(
    ) {
        let input_parser = InputParser();

        input_parser.parse_query(String::from(
            "INSERT INTO users(name,email, number) VALUES ('felix', 'felix@gmail.de', 12345), ('paul', 'paul@mail.com');",
        ));
    }
}
