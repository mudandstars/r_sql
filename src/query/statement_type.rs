use std::str::Split;

#[derive(Debug)]
pub enum StatementType {
    Invalid,
    Select,
    Insert,
    CreateTable,
}

impl StatementType {
    pub fn new(query: &mut Split<'_, char>) -> Self {
        let first_word = query.next().expect("Invalid query.");

        let statement_type = StatementType::from(first_word);

        if statement_type.id() == StatementType::Invalid.id() {
            let second_word = query.next().expect("Invalid query.");
            let first_two_words = format!("{} {}", first_word, second_word);

            let statement_type = StatementType::from(first_two_words.as_str());

            if statement_type.id() == StatementType::Invalid.id() {
                panic!("Unimplemented Command. Please use 'SELECT', 'CREATE TABLE' or 'INSERT'");
            } else {
                statement_type
            }
        } else {
            statement_type
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            StatementType::Invalid => 0,
            StatementType::Select => 1,
            StatementType::Insert => 2,
            StatementType::CreateTable => 3,
        }
    }
}

impl From<&str> for StatementType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_ref() {
            "SELECT" => StatementType::Select,
            "INSERT INTO" => StatementType::Insert,
            "CREATE TABLE" => StatementType::CreateTable,
            _ => StatementType::Invalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_select_command_correctly() {
        let query_type = StatementType::new(&mut "select".split(' '));
        assert_eq!(query_type.id(), StatementType::Select.id());

        let query_type = StatementType::new(&mut "SELECT".split(' '));
        assert_eq!(query_type.id(), StatementType::Select.id());

        let query_type = StatementType::new(&mut "sElEcT".split(' '));
        assert_eq!(query_type.id(), StatementType::Select.id());
    }

    #[test]
    fn test_creates_insert_command_correctly() {
        let query_type = StatementType::new(&mut "insert into".split(' '));
        assert_eq!(query_type.id(), StatementType::Insert.id());
    }

    #[test]
    fn test_creates_create_table_command_correctly() {
        let query_type = StatementType::new(&mut "create table".split(' '));
        assert_eq!(query_type.id(), StatementType::CreateTable.id());
    }
}
