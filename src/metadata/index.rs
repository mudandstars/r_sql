use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    pub name: String,
    pub column_name: String,
}

impl Index {
    pub fn new(column_name: &str) -> Self {
        Index {
            name: format!("{}_index", column_name),
            column_name: String::from(column_name),
        }
    }
}
