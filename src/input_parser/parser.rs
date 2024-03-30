use crate::query::Query;
use crate::query::QueryType;

pub struct InputParser();

impl InputParser {
    pub fn parse_query(self, input: String) -> Query {
        let mut input_vector = input.trim().split(' ');

        if !input_vector
            .clone()
            .last()
            .expect("Query is required.")
            .ends_with(';')
        {
            panic!("Your statement must end with a semicolon.")
        }

        let query_type = QueryType::from(input_vector.next().expect("Invalid query."));

        let mut selection: Vec<String> = Vec::new();
        let mut current_word = "";

        while true {
            let current_slice_vector: Vec<&str> = input_vector
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

        let table_name = input_vector
            .next()
            .expect("Invalid query.")
            .replace(';', "");

        Query::new(input, query_type, selection, table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create_a_parsed_input_from_a_simple_select_query() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from("SELECT * FROM users;"));

        assert_eq!(query._type.type_id(), QueryType::Select.type_id());
        assert_eq!(query.selection.first().unwrap(), "*");
        assert_eq!(query.table_name, String::from("users"));
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_for_specific_columns() {
        let input_parser = InputParser();
        let query = input_parser.parse_query(String::from(
            "SELECT id,foreign_id, number,name,job, another FROM users;",
        ));

        assert_eq!(query._type.type_id(), QueryType::Select.type_id());
        assert_eq!(query.table_name, String::from("users"));

        dbg!(&query.selection);
        assert_eq!(query.selection[0], "id");
        assert_eq!(query.selection[1], "foreign_id");
        assert_eq!(query.selection[2], "number");
        assert_eq!(query.selection[3], "name");
        assert_eq!(query.selection[4], "job");
        assert_eq!(query.selection[5], "another");
    }
}
