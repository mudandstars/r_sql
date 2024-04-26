mod binary_engine;
mod dynamic_record;
mod file_paths;

use crate::metadata;
use crate::sql_parser::query::Statement;
use crate::{engine::binary_engine::BinaryEngine, sql_parser::query::Query};

pub fn io_engine_factory(storage_type: self::Type) -> Box<dyn self::Engine> {
    match storage_type {
        self::Type::Binary => Box::new(BinaryEngine::new()),
    }
}

pub trait Engine {
    fn execute(&self, query: Query) -> EngineResult {
        match query.statement {
            Statement::CreateTable {
                table_name,
                columns,
            } => self.create_table(table_name, columns),
            Statement::Select {
                table_name,
                selection,
                where_clauses,
            } => self.select(table_name, selection),
            Statement::InsertInto {
                table_name,
                column_names,
                values,
            } => self.insert(table_name, column_names, values),
        }
    }

    fn select(&self, table_name: String, column_names: Vec<String>) -> EngineResult;
    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> EngineResult;
    fn insert(
        &self,
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> EngineResult;
}

pub enum Type {
    Binary,
}

pub struct EngineResponse {
    records: Option<Vec<dynamic_record::DynamicRecord>>,
    table: Option<metadata::Table>,
}

pub type EngineResult = std::result::Result<EngineResponse, String>;
