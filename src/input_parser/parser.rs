use std::str::Split;

use crate::query::Query;

use crate::query::QueryType;

pub struct InputParser();

impl InputParser {
    pub fn parse_query(self, input: String) -> Query {
        let cleaned_input = input.trim().replace('\n', "");
        let mut query_iter = cleaned_input.split(' ');

        if !query_iter
            .clone()
            .last()
            .expect("Query is required.")
            .ends_with(';')
        {
            panic!("Your statement must end with a semicolon.")
        }

        let query_type = QueryType::new(&mut query_iter);

        match query_type {
            QueryType::Select => {
                self.parse_select_statement(input.clone(), &mut query_iter, query_type)
            }
            QueryType::CreateTable => {
                self.parse_create_table_statement(input.clone(), &mut query_iter, query_type)
            }
            _ => panic!("Not implemented yet."),
        }
    }

    fn parse_select_statement(
        self,
        input: String,
        query_iter: &mut Split<'_, char>,
        query_type: QueryType,
    ) -> Query {
        let mut selection: Vec<String> = Vec::new();
        let mut current_word = "";

        while true {
            let current_slice_vector: Vec<&str> = query_iter
                .next()
                .expect("Invalid query.")
                .split(',')
                .collect();

            for word in current_slice_vector {
                if !word.is_empty() {
                    current_word = word;

                    if word != "FROM" {
                        selection.push(String::from(word));
                    }
                }
            }

            if current_word.to_uppercase() == "FROM" {
                break;
            }
        }

        let table_name = query_iter.next().expect("Invalid query.").replace(';', "");

        Query::new(input, query_type, Some(selection), None, table_name)
    }

    fn parse_create_table_statement(
        self,
        input: String,
        query_iter: &mut Split<'_, char>,
        query_type: QueryType,
    ) -> Query {
        let table_name = query_iter
            .next()
            .expect("Invalid query.")
            .split('(')
            .next()
            .expect("Invalid query.")
            .to_string();
        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut current_column: Vec<String> = Vec::new();
        let mut current_word = "";

        while true {
            while true {
                if current_word.ends_with(',') {
                    columns.push(current_column);
                    current_column = Vec::new();
                    break;
                }

                current_word = query_iter.next().expect("Invalid query.");
                current_column.push(current_word.replace(',', ""));
            }
            if current_word == ");" {
                break;
            }
        }

        Query::new(input, query_type, None, Some(columns), table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create_a_parsed_input_from_a_simple_select_query() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from("SELECT * FROM users;"));

        assert_eq!(query._type.id(), QueryType::Select.id());
        assert_eq!(query.selection.unwrap().first().unwrap(), "*");
        assert_eq!(query.table_name, String::from("users"));
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_for_specific_columns() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from(
            "SELECT id,foreign_id, number,name,job, another FROM users;",
        ));

        assert_eq!(query._type.id(), QueryType::Select.id());
        assert_eq!(query.table_name, String::from("users"));

        let selection = query.selection.unwrap();

        assert_eq!(selection[0], "id");
        assert_eq!(selection[1], "foreign_id");
        assert_eq!(selection[2], "number");
        assert_eq!(selection[3], "name");
        assert_eq!(selection[4], "job");
        assert_eq!(selection[5], "another");
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

        assert_eq!(query._type.id(), QueryType::CreateTable.id());
        assert_eq!(query.table_name, String::from("users"));

        let columns = query.columns.unwrap();

        assert_eq!(columns[0][0], String::from("id"));
        assert_eq!(columns[0][1], String::from("PRIMARY KEY"));

        assert_eq!(columns[1][0], String::from("name"));
        assert_eq!(columns[1][1], String::from("VARCHAR"));

        assert_eq!(columns[2][0], String::from("email"));
        assert_eq!(columns[1][1], String::from("VARCHAR"));
    }
}
