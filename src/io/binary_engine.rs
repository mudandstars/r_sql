use super::Engine;
use crate::metadata;
use crate::query::{Query, Statement};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections;
use std::fs;
use std::io::{self, Read, Write};
use std::path;

#[derive(Serialize, Deserialize, Debug)]
struct DynamicRecord {
    data: serde_json::Value,
}

pub struct BinaryEngine {
    base_path: String,
}

struct EngineResponse {
    records: Option<Vec<DynamicRecord>>,
    table: Option<metadata::Table>,
}

impl Engine for BinaryEngine {
    fn execute(&self, query: Query) {
        println!("\tWriting in binary..");

        let result = match query.statement {
            Statement::CreateTable {
                table_name,
                columns,
            } => self.create_table(table_name, columns),
            Statement::Select {
                table_name,
                selection,
            } => self.select(table_name, selection),
            Statement::InsertInto {
                table_name,
                column_names,
                values,
            } => self.insert(table_name, column_names, values),
        };

        if result.is_err() {
            super::raise_error("\tERROR: An error occured while trying to write..");
        }
    }
}

impl BinaryEngine {
    pub fn new() -> Self {
        dotenv().ok();

        let database_base_dir =
            std::env::var("DATABASE_BASE_DIR").expect("DATABASE_BASE_DIR must be set");

        BinaryEngine {
            base_path: database_base_dir,
        }
    }

    fn create_table(
        &self,
        table_name: String,
        columns: Vec<Vec<String>>,
    ) -> io::Result<EngineResponse> {
        let table_path = String::from(&self.base_path) + "/" + &table_name;

        if !path::Path::new(&table_path).exists() {
            fs::create_dir(table_path.clone()).expect("\tFailed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name.clone(), columns.clone());
        self.store_meta_data(&table)
            .expect("\tFailed to store meta-data.");

        fs::File::create(table_path + "/data_page_1.bin")?;

        Ok(EngineResponse {
            table: Some(table),
            records: None,
        })
    }

    fn store_meta_data(&self, table: &metadata::Table) -> io::Result<()> {
        let path = format!("{}/{}/metadata.bin", self.base_path, table.name);

        let serialized_table = &bincode::serialize(table).unwrap();

        let mut file = fs::File::create(path)?;
        file.write_all(serialized_table)?;

        Ok(())
    }

    fn load_meta_data(&self, table_name: &str) -> io::Result<metadata::Table> {
        let path = format!("{}/{}/metadata.bin", self.base_path, table_name);

        let mut file = fs::File::open(path)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let metadata: metadata::Table = bincode::deserialize(&buffer[..]).unwrap();

        Ok(metadata)
    }

    fn insert(
        &self,
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> io::Result<EngineResponse> {
        let metadata = self.load_meta_data(&table_name);

        if metadata.is_err() {
            super::raise_error(format!("Table '{}' does not exist.", table_name).as_str())
        }

        let metadata_columns = metadata.unwrap().columns;

        for value_vec in values {
            let mut dynamic_data = collections::HashMap::new();

            for (index, column_name) in column_names.iter().enumerate() {
                for metadata_column in &metadata_columns {
                    if metadata_column.name == column_name.as_str() {
                        dynamic_data
                            .insert(column_name.to_string(), serde_json::json!(value_vec[index]));
                    }
                }
            }

            let data = serde_json::Value::Object(dynamic_data.into_iter().collect());
            let record = DynamicRecord { data };

            self.save_record(&record, &table_name)?;
        }

        Ok(EngineResponse {
            table: None,
            records: None,
        })
    }

    fn save_record(&self, record: &DynamicRecord, table_name: &str) -> std::io::Result<()> {
        let file_path = format!("{}/{}/metadata.bin", self.base_path, table_name);
        let file_path = file_path.as_str();
        if !path::Path::new(file_path).exists() {
            fs::create_dir(file_path)
                .expect("\tFailed to create new data page when attempting to store data.");
        }

        let serialized = bincode::serialize(record).unwrap();

        let mut file = fs::File::create(file_path)?;
        file.write_all(&serialized)?;

        Ok(())
    }

    fn select(&self, table_name: String, column_names: Vec<String>) -> io::Result<EngineResponse> {
        let records = self.load_table_contents(&table_name)?;

        Ok(EngineResponse {
            records: Some(records),
            table: None,
        })
    }

    fn load_table_contents(&self, table_name: &str) -> io::Result<Vec<DynamicRecord>> {
        let path = format!("{}/{}/data_page_1.bin", self.base_path, table_name);

        let mut file = fs::File::open(path)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let records: Vec<DynamicRecord> = bincode::deserialize(&buffer[..]).unwrap();

        Ok(records)
    }
}

impl Default for BinaryEngine {
    fn default() -> Self {
        BinaryEngine {
            base_path: String::from("/Users/paul/r_sql/"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_write_metadata_to_disk() {
        let engine = BinaryEngine::new();
        let table_name = "test_table";

        engine
            .create_table(
                table_name.to_string(),
                vec![
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec!["email".to_string(), "VARCHAR".to_string()],
                ],
            )
            .unwrap();

        let table = engine.load_meta_data(table_name).unwrap();

        assert_eq!(table.name, table_name);
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
    fn test_can_insert_into_table() {
        let engine = BinaryEngine::new();
        let table_name = "test_table";

        engine
            .create_table(
                table_name.to_string(),
                vec![
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec!["email".to_string(), "VARCHAR".to_string()],
                ],
            )
            .unwrap();

        engine
            .insert(
                table_name.to_string(),
                vec!["name".to_string(), "email".to_string()],
                vec![
                    vec!["john".to_string(), "john@mail.com".to_string()],
                    vec!["doe".to_string(), "doe@mail.com".to_string()],
                ],
            )
            .unwrap();
    }
}
