mod binary_engine;
mod dynamic_record;
mod engine_response;

use crate::{engine::binary_engine::BinaryEngine, query::Query};

pub fn io_engine_factory(storage_type: self::Type) -> Box<dyn self::Engine> {
    match storage_type {
        self::Type::Binary => Box::new(BinaryEngine::new()),
    }
}

pub trait Engine {
    fn execute(&self, query: Query) -> engine_response::EngineResponse;
}

pub enum Type {
    Binary,
}

fn raise_error(error: &str) {
    panic!("{}", error);
}
