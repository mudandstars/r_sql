mod query_type;

pub use crate::query::query_type::QueryType;

pub struct Query {
    pub text: String,
    pub _type: QueryType,
    pub selection: Option<Vec<String>>,
    pub columns: Option<Vec<Vec<String>>>,
    pub table_name: String,
    pub subqueries: Option<Vec<Query>>,
}

impl Query {
    pub fn new(
        text: String,
        _type: QueryType,
        selection: Option<Vec<String>>,
        columns: Option<Vec<Vec<String>>>,
        table_name: String,
    ) -> Self {
        Query {
            text,
            _type,
            selection,
            columns,
            table_name,
            subqueries: None,
        }
    }
}
