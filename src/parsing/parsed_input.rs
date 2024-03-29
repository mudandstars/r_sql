use super::command::Command;

pub struct ParsedInput {
    command: Command,
    selection: Vec<String>,
    table: String,
}

impl ParsedInput {
    pub fn new(input: String) -> Self {
        let mut input_vector = input.trim().split(' ');

        if !input_vector
            .clone()
            .last()
            .expect("Query is required.")
            .ends_with(';')
        {
            panic!("Your statement must end with a semicolon.")
        }

        let command = Command::from(input_vector.next().expect("Invalid query."));

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

        let table = input_vector
            .next()
            .expect("Invalid query.")
            .replace(';', "");

        ParsedInput {
            command,
            selection,
            table,
        }
    }

    pub fn execute(self) {
        println!("Executing query..")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create_a_parsed_input_from_a_simple_select_query() {
        let parsed_input = ParsedInput::new(String::from("SELECT * FROM users;"));

        assert_eq!(parsed_input.command.type_id(), Command::Select.type_id());
        assert_eq!(parsed_input.selection.first().unwrap(), "*");
        assert_eq!(parsed_input.table, String::from("users"));
    }

    #[test]
    fn test_can_create_a_parsed_input_from_a_select_query_for_specific_columns() {
        let parsed_input = ParsedInput::new(String::from(
            "SELECT id,foreign_id, number,name,job, another FROM users;",
        ));

        assert_eq!(parsed_input.command.type_id(), Command::Select.type_id());
        assert_eq!(parsed_input.table, String::from("users"));

        dbg!(&parsed_input.selection);
        assert_eq!(parsed_input.selection[0], "id");
        assert_eq!(parsed_input.selection[1], "foreign_id");
        assert_eq!(parsed_input.selection[2], "number");
        assert_eq!(parsed_input.selection[3], "name");
        assert_eq!(parsed_input.selection[4], "job");
        assert_eq!(parsed_input.selection[5], "another");
    }
}
