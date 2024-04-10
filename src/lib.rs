pub mod engine;
pub mod metadata;
pub mod query;
pub mod sql_parser;

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

    pub fn execute(&self, query: String) {
        println!("\tExecuting query..");

        let query = self.input_parser.parse_query(query);

        self.io_engine.execute(query);
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
