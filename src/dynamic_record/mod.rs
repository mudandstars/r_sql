use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DynamicRecord {
    pub fields: HashMap<String, Value>,
}

impl DynamicRecord {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        DynamicRecord { fields }
    }

    pub fn filter_columns(&mut self, column_names: &[String]) {
        let filtered_map: HashMap<String, Value> = self
            .fields
            .iter()
            .filter(|(key, _)| column_names.contains(key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(); // Collect into a HashMap

        self.fields = filtered_map; // Update self.fields with the filtered map
    }

    pub fn entry_should_be_included(
        &self,
        where_clauses: &Option<&HashMap<String, String>>,
    ) -> bool {
        match where_clauses {
            None => true,
            Some(where_clauses) => {
                for key in where_clauses.keys() {
                    if self.fields.contains_key(key) {
                        return self
                            .fields
                            .get(key)
                            .unwrap()
                            .fullfills(where_clauses.get(key).unwrap());
                    }
                }

                true
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Value {
    Int(i32),
    Text(String),
}

impl Value {
    pub fn fullfills(&self, value_to_match: &str) -> bool {
        match self {
            Self::Int(value) => value_to_match == value.to_string(),
            Self::Text(value) => value_to_match == value,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(integer) => write!(f, "{}", integer),
            Value::Text(text) => write!(f, "{}", text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_compare_int_values() {
        assert!(Value::Int(10).fullfills("10"));
        assert!(!Value::Int(10).fullfills("11"));
    }

    #[test]
    fn test_can_compare_string_values() {
        assert!(Value::Text(String::from("here")).fullfills("here"));
        assert!(!Value::Text(String::from("here")).fullfills("here "));
    }
}
