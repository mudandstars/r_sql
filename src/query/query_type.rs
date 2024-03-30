use std::str::Split;

#[derive(Debug)]
pub enum QueryType {
    Invalid,
    Select,
    Insert,
    CreateTable,
}

impl QueryType {
    pub fn new(query: &mut Split<'_, char>) -> Self {
        let first_word = query.next().expect("Invalid query.");

        let query_type = QueryType::from(first_word);

        if query_type.id() == QueryType::Invalid.id() {
            let second_word = query.next().expect("Invalid query.");
            let first_two_words = format!("{} {}", first_word, second_word);

            QueryType::from(first_two_words.as_str())
        } else {
            query_type
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            QueryType::Invalid => 0,
            QueryType::Select => 1,
            QueryType::Insert => 2,
            QueryType::CreateTable => 3,
        }
    }
}

impl From<&str> for QueryType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_ref() {
            "SELECT" => QueryType::Select,
            "INSERT INTO" => QueryType::Insert,
            "CREATE TABLE" => QueryType::CreateTable,
            _ => QueryType::Invalid,
            // _ => panic!("Unimplemented Command. Please use 'SELECT', 'CREATE TABLE' or 'INSERT'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_select_command_correctly() {
        let query_type = QueryType::new(&mut "select".split(' '));
        assert_eq!(query_type.id(), QueryType::Select.id());

        let query_type = QueryType::new(&mut "SELECT".split(' '));
        assert_eq!(query_type.id(), QueryType::Select.id());

        let query_type = QueryType::new(&mut "sElEcT".split(' '));
        assert_eq!(query_type.id(), QueryType::Select.id());
    }

    #[test]
    fn test_creates_insert_command_correctly() {
        let query_type = QueryType::new(&mut "insert into".split(' '));
        assert_eq!(query_type.id(), QueryType::Insert.id());
    }

    #[test]
    fn test_creates_create_table_command_correctly() {
        let query_type = QueryType::new(&mut "create table".split(' '));
        assert_eq!(query_type.id(), QueryType::CreateTable.id());
    }
}
