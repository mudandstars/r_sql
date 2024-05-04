pub trait CreateIndex {
    fn create_index(
        &self,
        table_name: String,
        column_name: String,
        index_name: String,
    ) -> super::EngineResult;
}
