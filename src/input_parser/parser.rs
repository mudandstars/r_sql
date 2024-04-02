use crate::query::Query;

use crate::query::StatementType;

use super::statement_parser::statement_parser_factory;

pub struct InputParser();

impl InputParser {
    pub fn parse_query(&self, input: String) -> Query {
        let cleaned_input = input.trim().replace('\n', "");
        let mut query_iterator = cleaned_input.split(' ');

        if !query_iterator
            .clone()
            .last()
            .expect("Query is required.")
            .ends_with(';')
        {
            panic!("Your statement must end with a semicolon.")
        }

        let statement_type = StatementType::new(&mut query_iterator);

        let statement_parser = statement_parser_factory(statement_type);

        Query::new(input, statement_parser.parse_statement(&mut query_iterator))
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
}
