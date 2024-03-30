mod statement;
mod statement_type;

pub use crate::query::statement::Statement;
pub use crate::query::statement_type::StatementType;

pub struct Query {
    pub text: String,
    pub statement: Statement,
}

impl Query {
    pub fn new(text: String, statement: Statement) -> Self {
        Query { text, statement }
    }
}
