pub enum QueryType {
    Select,
    Insert,
}

impl QueryType {
    pub fn type_id(self) -> u8 {
        match self {
            QueryType::Select => 1,
            QueryType::Insert => 2,
        }
    }
}

impl From<&str> for QueryType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_ref() {
            "SELECT" => QueryType::Select,
            "INSERT INTO" => QueryType::Insert,
            _ => panic!("Unimplemented Command. Please use 'SELECT' or 'INSERT'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_select_command_correctly() {
        assert_eq!(
            QueryType::from("select").type_id(),
            QueryType::Select.type_id()
        );
        assert_eq!(
            QueryType::from("SELECT").type_id(),
            QueryType::Select.type_id()
        );
        assert_eq!(
            QueryType::from("sElEcT").type_id(),
            QueryType::Select.type_id()
        );
    }

    #[test]
    fn test_creates_insert_command_correctly() {
        assert_eq!(
            QueryType::from("insert into").type_id(),
            QueryType::Insert.type_id()
        );
        assert_eq!(
            QueryType::from("INSERT INTO").type_id(),
            QueryType::Insert.type_id()
        );
    }
}
