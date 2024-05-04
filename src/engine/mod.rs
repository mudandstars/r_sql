mod binary_engine;
mod create_index;
mod create_table;
mod dynamic_record;
mod file_paths;
mod insert;
mod select;

use crate::metadata;
use crate::sql_parser::query::Statement;
use crate::{engine::binary_engine::BinaryEngine, sql_parser::query::Query};

use self::create_index::CreateIndex;
use self::create_table::CreateTable;
use self::insert::Insert;
use self::select::Select;

pub fn io_engine_factory(storage_type: self::Type) -> Box<dyn self::Engine> {
    match storage_type {
        self::Type::Binary => Box::new(BinaryEngine::new()),
    }
}

pub trait Engine: Select + CreateIndex + CreateTable + Insert {
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
            } => self.select(table_name, selection, where_clauses),
            Statement::InsertInto {
                table_name,
                column_names,
                values,
            } => self.insert(table_name, column_names, values),
            Statement::CreateIndex {
                table_name,
                column_name,
                index_name,
            } => self.create_index(table_name, column_name, index_name),
        }
    }
}

pub enum Type {
    Binary,
}

pub struct EngineResponse {
    records: Option<Vec<dynamic_record::DynamicRecord>>,
    table: Option<metadata::Table>,
}

pub type EngineResult = std::result::Result<EngineResponse, String>;
