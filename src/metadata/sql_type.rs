pub const VARCHAR: &str = "VARCHAR";
pub const INTEGER: &str = "INTEGER";

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum SqlType {
    Varchar,
    Integer,
}

impl From<String> for SqlType {
    fn from(value: String) -> Self {
        match value.to_uppercase().as_ref() {
            VARCHAR => Self::Varchar,
            INTEGER => Self::Integer,
            _ => panic!("Not a valid type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SqlType;

    #[should_panic]
    #[test]
    fn test_panics_on_invalid_option() {
        let _type = SqlType::from("varcha".to_string());
    }

    #[test]
    fn test_can_parse_a_varchar() {
        match SqlType::from("varchar".to_string()) {
            SqlType::Varchar => {}
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn test_can_parse_an_integer() {
        match SqlType::from("integer".to_string()) {
            SqlType::Integer => {}
            _ => panic!("wrong type"),
        }
    }
}
