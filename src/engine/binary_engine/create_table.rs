use std::{fs, path};

use crate::{engine::create_table::CreateTable, metadata};

impl CreateTable for super::BinaryEngine {
    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> super::EngineResult {
        let table_path = self.file_paths.table_path(&table_name);

        if !path::Path::new(&table_path).exists() {
            fs::create_dir(table_path.clone()).expect("\tFailed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name, columns);

        self.table_manager
            .store_meta_data(&table)
            .expect("\tFailed to store meta-data.");

        Ok(super::EngineResponse {
            table: Some(table),
            records: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::engine::{CreateTable, Insert, Select};
    use crate::{engine::binary_engine::BinaryEngine, io_test_context::FileTestContext};

    #[test]
    fn test_can_create_a_table_and_write_metadata_to_disk_correctly() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![
                    vec!["unique_id".to_string(), "PRIMARY KEY".to_string()],
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec!["email".to_string(), "VARCHAR".to_string()],
                ],
            )
            .unwrap();

        let table = engine
            .table_manager
            .load_meta_data(context.table_name())
            .unwrap();
        assert_eq!(table.primary_key.name, "unique_id");
        match table.primary_key.data_type {
            metadata::SqlType::Integer => {}
            _ => panic!("failed"),
        }
        assert!(!table.primary_key.nullable);

        assert_eq!(table.name, context.table_name());
        assert_eq!(table.columns.first().unwrap().name, "name");
        match table.columns.first().unwrap().data_type {
            metadata::SqlType::Varchar => {}
            _ => panic!("failed"),
        }
        assert_eq!(table.columns.last().unwrap().name, "email");
        match table.columns.last().unwrap().data_type {
            metadata::SqlType::Varchar => {}
            _ => panic!("failed"),
        }
    }

    #[test]
    fn test_creates_a_primary_key_id_if_none_given() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![vec!["name".to_string(), "VARCHAR".to_string()]],
            )
            .unwrap();

        let table = engine
            .table_manager
            .load_meta_data(context.table_name())
            .unwrap();

        assert_eq!(table.primary_key.name, "id");
        match table.primary_key.data_type {
            metadata::SqlType::Integer => {}
            _ => panic!("failed"),
        }
        assert!(!table.primary_key.nullable);
    }

    #[test]
    fn test_sets_primary_key_correctly_on_create_table_command() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec![
                        "email".to_string(),
                        "VARCHAR".to_string(),
                        "PRIMARY KEY".to_string(),
                    ],
                ],
            )
            .unwrap();

        let table = engine
            .table_manager
            .load_meta_data(context.table_name())
            .unwrap();

        assert_eq!(table.primary_key.name, "email");
        match table.primary_key.data_type {
            metadata::SqlType::Varchar => {}
            _ => panic!("failed"),
        }
        assert!(!table.primary_key.nullable);
    }

    #[test]
    fn test_creates_an_index_on_the_primary_key() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![vec!["name".to_string(), "VARCHAR".to_string()]],
            )
            .unwrap();

        let table = engine
            .table_manager
            .load_meta_data(context.table_name())
            .unwrap();

        assert_eq!(table.indices.len(), 1);
        assert_eq!(table.indices.first().unwrap().column_name, "id");
    }

    #[test]
    fn test_automatically_increments_primary_key_if_none_given() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![vec!["name".to_string(), "VARCHAR".to_string()]],
            )
            .unwrap();

        engine
            .insert(
                context.table_name().to_string(),
                vec!["name".to_string()],
                vec![vec!["john".to_string()], vec!["doe".to_string()]],
            )
            .unwrap();

        let records = engine
            .select(context.table_name().to_string(), vec![], HashMap::new())
            .unwrap()
            .records
            .unwrap();

        assert!(records
            .first()
            .unwrap()
            .fields
            .get("id")
            .unwrap()
            .fullfills("1"));
        assert!(records
            .last()
            .unwrap()
            .fields
            .get("id")
            .unwrap()
            .fullfills("2"));
    }
}
