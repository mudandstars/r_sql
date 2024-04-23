use serde::{Deserialize, Serialize};

use super::SqlType;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
}
