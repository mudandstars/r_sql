use crate::{engine, sql_parser};

pub struct SQLEngine {
    input_parser: sql_parser::SqlParser,
    io_engine: Box<dyn engine::Engine>,
}

impl SQLEngine {
    pub fn new(io_type: engine::Type) -> Self {
        SQLEngine {
            input_parser: sql_parser::SqlParser(),
            io_engine: engine::io_engine_factory(io_type),
        }
    }

    pub fn execute(&self, query: String) -> engine::EngineResponse {
        println!("\tExecuting query..");

        let query = self.input_parser.parse_query(query);

        match query {
            Ok(query) => self.io_engine.execute(query),
            Err(message) => engine::EngineResponse::Error { message },
        }
    }
}

impl Default for SQLEngine {
    fn default() -> Self {
        SQLEngine {
            input_parser: sql_parser::SqlParser(),
            io_engine: engine::io_engine_factory(engine::Type::Binary),
        }
    }
}
