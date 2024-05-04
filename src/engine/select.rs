use std::collections::HashMap;

pub trait Select {
    fn select(
        &self,
        table_name: String,
        column_names: Vec<String>,
        where_clauses: HashMap<String, String>,
    ) -> super::EngineResult;
}
