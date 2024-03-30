pub mod input_parser;
pub mod io;
pub mod query;

pub struct SQLEngine {
    input_parser: input_parser::InputParser,
    io_engine: Box<dyn io::Engine>,
}

impl SQLEngine {
    pub fn new(io_type: io::Type) -> Self {
        SQLEngine {
            input_parser: input_parser::InputParser(),
            io_engine: io::io_engine_factory(io_type),
        }
    }

    pub fn execute(self, query: String) {
        println!("Executing query..");

        let query = self.input_parser.parse_query(query);

        // self.io_engine.execute(query);
    }
}

impl Default for SQLEngine {
    fn default() -> Self {
        SQLEngine {
            input_parser: input_parser::InputParser(),
            io_engine: io::io_engine_factory(io::Type::Binary),
        }
    }
}
