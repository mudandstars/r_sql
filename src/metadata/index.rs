use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    pub name: String,
    pub column_name: String,
    tree: BTreeMap<String, usize>,
}

impl Index {
    pub fn new(column_name: &str) -> Self {
        Index {
            name: format!("{}_index", column_name),
            column_name: String::from(column_name),
            tree: BTreeMap::new(),
        }
    }

    pub fn update_tree(&mut self, values: (String, usize)) {
        self.tree.insert(values.0, values.1);
    }

    pub fn data_page(&self, key: &str) -> std::result::Result<String, String> {
        let value = self.tree.get(key);

        match value {
            Some(value) => Ok(format!("data_page_{}", value)),
            None => Err(String::from("Key does not exist.")),
        }
    }
}
