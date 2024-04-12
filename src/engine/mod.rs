mod binary_engine;
mod dynamic_record;

use crate::metadata;
use crate::{engine::binary_engine::BinaryEngine, sql_parser::query::Query};

pub fn io_engine_factory(storage_type: self::Type) -> Box<dyn self::Engine> {
    match storage_type {
        self::Type::Binary => Box::new(BinaryEngine::new()),
    }
}

pub trait Engine {
    fn execute(&self, query: Query) -> EngineResult;
}

pub enum Type {
    Binary,
}

pub struct EngineResponse {
    records: Option<Vec<dynamic_record::DynamicRecord>>,
    table: Option<metadata::Table>,
}

pub type EngineResult = std::result::Result<EngineResponse, String>;
