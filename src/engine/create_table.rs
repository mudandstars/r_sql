pub trait CreateTable {
    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> super::EngineResult;
}
