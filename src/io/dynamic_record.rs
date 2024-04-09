use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

#[derive(Serialize, Deserialize, Debug)]
pub struct DynamicRecord {
    pub fields: HashMap<String, Value>,
}

impl DynamicRecord {
    pub fn new(fields: HashMap<String, Value>) -> Self {
        DynamicRecord { fields }
    }

    pub fn filter_columns(&mut self, column_names: &Vec<String>) {
        let filtered_map: HashMap<String, Value> = self
            .fields
            .iter()
            .filter(|(k, _)| column_names.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(); // Collect into a HashMap

        self.fields = filtered_map; // Update self.fields with the filtered map
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Value {
    Int(i32),
    Text(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(integer) => write!(f, "{}", integer),
            Value::Text(text) => write!(f, "{}", text),
        }
    }
}
