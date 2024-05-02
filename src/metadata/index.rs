use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Index {
    pub name: String,
    pub column_name: String,
    tree: BTreeMap<String, Vec<usize>>,
}

impl Index {
    pub fn new(index_name: String, column_name: &str) -> Self {
        Index {
            name: index_name,
            column_name: String::from(column_name),
            tree: BTreeMap::new(),
        }
    }

    pub fn update_tree(&mut self, values: (String, usize)) {
        if self.tree.contains_key(&values.0) {
            let previous_values = self.tree.get(&values.0).unwrap();
            let mut new_values = previous_values.clone();
            new_values.push(values.1);

            self.tree.insert(values.0, new_values);
        } else {
            self.tree.insert(values.0, vec![values.1]);
        }
    }

    pub fn data_page_indices(&self, key: &str) -> std::result::Result<Vec<usize>, String> {
        let value = self.tree.get(key);

        match value {
            Some(value) => Ok(value.clone()),
            None => Err(String::from("Key does not exist.")),
        }
    }
}
