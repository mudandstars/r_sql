#[derive(Debug)]
pub enum StatementType {
    Invalid,
    Select,
    InsertInto,
    CreateTable,
    CreateIndex,
}

impl StatementType {
    pub fn new(first_grapheme: &str, second_grapheme: &str) -> Self {
        let statement_type = StatementType::from(first_grapheme);

        if statement_type.id() == StatementType::Invalid.id() {
            let first_two_words = format!("{} {}", first_grapheme, second_grapheme);

            let statement_type = StatementType::from(first_two_words.as_str());

            statement_type
        } else {
            statement_type
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            StatementType::Invalid => 0,
            StatementType::Select => 1,
            StatementType::InsertInto => 2,
            StatementType::CreateTable => 3,
            StatementType::CreateIndex => 4,
        }
    }
}

impl From<&str> for StatementType {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_ref() {
            "SELECT" => StatementType::Select,
            "INSERT INTO" => StatementType::InsertInto,
            "CREATE TABLE" => StatementType::CreateTable,
            "CREATE INDEX" => StatementType::CreateIndex,
            _ => StatementType::Invalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_select_command_correctly() {
        let query_type = StatementType::new("select", "asdfasdf");
        assert_eq!(query_type.id(), StatementType::Select.id());

        let query_type = StatementType::new("SELECT", "sadf");
        assert_eq!(query_type.id(), StatementType::Select.id());

        let query_type = StatementType::new("sElEcT", "1209nasdc");
        assert_eq!(query_type.id(), StatementType::Select.id());
    }

    #[test]
    fn test_creates_insert_command_correctly() {
        let query_type = StatementType::new("insert", "into");
        assert_eq!(query_type.id(), StatementType::InsertInto.id());
    }

    #[test]
    fn test_creates_create_table_command_correctly() {
        let query_type = StatementType::new("create", "table");
        assert_eq!(query_type.id(), StatementType::CreateTable.id());
    }

    #[test]
    fn test_creates_create_index_command_correctly() {
        let query_type = StatementType::new("create", "index");
        assert_eq!(query_type.id(), StatementType::CreateIndex.id());
    }
}
