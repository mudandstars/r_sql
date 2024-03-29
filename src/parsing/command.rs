pub enum Command {
    Select,
    Insert,
}

impl Command {
    pub fn type_id(self) -> u8 {
        match self {
            Command::Select => 1,
            Command::Insert => 2,
        }
    }
}

impl From<&str> for Command {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_ref() {
            "SELECT" => Command::Select,
            "INSERT INTO" => Command::Insert,
            _ => panic!("Unimplemented Command. Please use 'SELECT' or 'INSERT'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creates_select_command_correctly() {
        assert_eq!(Command::from("select").type_id(), Command::Select.type_id());
        assert_eq!(Command::from("SELECT").type_id(), Command::Select.type_id());
        assert_eq!(Command::from("sElEcT").type_id(), Command::Select.type_id());
    }

    #[test]
    fn test_creates_insert_command_correctly() {
        assert_eq!(
            Command::from("insert into").type_id(),
            Command::Insert.type_id()
        );
        assert_eq!(
            Command::from("INSERT INTO").type_id(),
            Command::Insert.type_id()
        );
    }
}
