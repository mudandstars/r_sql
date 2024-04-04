mod binary_engine;

use crate::{io::binary_engine::BinaryEngine, query::Query};

pub fn io_engine_factory(storage_type: self::Type) -> Box<dyn self::Engine> {
    match storage_type {
        self::Type::Binary => Box::new(BinaryEngine::new()),
    }
}

pub trait Engine {
    fn execute(&self, query: Query);
}

pub enum Type {
    Binary,
}

fn raise_error(error: &str) {
    panic!("{}", error);
}
