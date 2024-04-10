use crate::engine::dynamic_record;
use crate::metadata;

pub enum EngineResponse {
    Success {
        records: Option<Vec<dynamic_record::DynamicRecord>>,
        table: Option<metadata::Table>,
    },
    Error {
        message: String,
    },
}
