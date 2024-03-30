mod query_type;

pub use crate::query::query_type::QueryType;

pub struct Query {
    pub text: String,
    pub _type: QueryType,
    pub selection: Vec<String>,
    pub table_name: String,
    pub subqueries: Option<Vec<Query>>,
}

impl Query {
    pub fn new(text: String, _type: QueryType, selection: Vec<String>, table_name: String) -> Self {
        Query {
            text,
            _type,
            selection,
            table_name,
            subqueries: None,
        }
    }
}
