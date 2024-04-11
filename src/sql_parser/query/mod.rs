mod statement;
mod statement_type;

pub use crate::sql_parser::query::statement::Statement;
pub use crate::sql_parser::query::statement_type::StatementType;

pub struct Query {
    pub text: String,
    pub statement: Statement,
}

impl Query {
    pub fn new(text: String, statement: Statement) -> Self {
        Query { text, statement }
    }
}

pub type QueryResult = std::result::Result<Query, String>;
