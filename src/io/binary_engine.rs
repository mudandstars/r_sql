use super::Engine;

pub struct BinaryEngine();

impl Engine for BinaryEngine {
    fn execute(self, query: crate::query::Query) {
        println!("Writing in binary..");
        println!("Executing query on table {}", query.table_name);
    }
}
