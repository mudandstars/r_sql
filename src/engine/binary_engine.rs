use super::dynamic_record;
use super::engine_response::EngineResponse;
use super::Engine;
use crate::metadata;
use crate::query::{Query, Statement};
use dotenvy::dotenv;
use std::collections;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path;
use std::path::Path;

pub struct BinaryEngine {
    base_path: String,
}

impl Engine for BinaryEngine {
    fn execute(&self, query: Query) -> EngineResponse {
        match query.statement {
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

    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> EngineResponse {
        let table_path = String::from(&self.base_path) + "/" + &table_name;

        if !path::Path::new(&table_path).exists() {
            fs::create_dir(table_path.clone()).expect("\tFailed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name.clone(), columns.clone());
        self.store_meta_data(&table)
            .expect("\tFailed to store meta-data.");

        EngineResponse::Success {
            table: Some(table),
            records: None,
        }
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

        let result: bincode::Result<metadata::Table> = bincode::deserialize(&buffer[..]);

        match result {
            Ok(table) => Ok(table),
            Err(e) => {
                println!("{}", e);
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "bincode serialization error",
                ))
            }
        }
    }

    fn insert(
        &self,
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> EngineResponse {
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
                        if metadata_column
                            .data_type
                            .allows_value(value_vec[index].clone())
                        {
                            dynamic_data.insert(
                                column_name.to_string(),
                                dynamic_record::Value::Text(value_vec[index].clone()),
                            );
                        } else {
                            super::raise_error(
                                format!("Type does not allow {} value", value_vec[index]).as_str(),
                            )
                        }
                    }
                }
            }

            let record = dynamic_record::DynamicRecord::new(dynamic_data);

            let result = self.save_record(record, &table_name);

            if let Err(err) = result {
                return EngineResponse::Error {
                    message: err.to_string(),
                };
            }
        }

        EngineResponse::Success {
            table: None,
            records: None,
        }
    }

    fn save_record(
        &self,
        record: dynamic_record::DynamicRecord,
        table_name: &str,
    ) -> std::io::Result<()> {
        let mut file_path;
        let mut data_page_index: u32 = 1;

        loop {
            file_path = format!(
                "{}/{}/data_page_{}.bin",
                self.base_path, table_name, data_page_index
            );

            if !Path::new(&file_path).exists() {
                break;
            }

            let path_size = fs::metadata(&file_path).unwrap().len();

            if path_size < 16000 {
                break;
            }

            data_page_index += 1;
        }

        let mut existing_contents: Vec<dynamic_record::DynamicRecord> = vec![];
        if Path::new(&file_path).exists() {
            existing_contents = self.load_records(file_path.as_str(), None).unwrap();
        }

        existing_contents.push(record);

        let serialized = bincode::serialize(&existing_contents).unwrap();

        let mut file = OpenOptions::new()
            .write(true)
            .append(false)
            .create(true)
            .open(&file_path)
            .unwrap();

        file.write_all(&serialized)?;

        Ok(())
    }

    fn select(&self, table_name: String, column_names: Vec<String>) -> EngineResponse {
        let records = self.load_table_contents(&table_name, column_names);

        match records {
            Ok(records) => EngineResponse::Success {
                records: Some(records),
                table: None,
            },
            Err(e) => EngineResponse::Error {
                message: e.to_string(),
            },
        }
    }

    fn load_table_contents(
        &self,
        table_name: &str,
        column_names: Vec<String>,
    ) -> io::Result<Vec<dynamic_record::DynamicRecord>> {
        let mut data_page_index = 1;
        let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

        loop {
            let path = format!(
                "{}/{}/data_page_{}.bin",
                self.base_path, table_name, data_page_index
            );

            if !path::Path::new(&path).exists() {
                break;
            }

            let mut file = fs::File::open(&path)?;

            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            records.extend(self.load_records(&path, Some(&column_names)).unwrap());

            data_page_index += 1;
        }

        Ok(records)
    }

    fn load_records(
        &self,
        path: &str,
        selected_columns: Option<&Vec<String>>,
    ) -> io::Result<Vec<dynamic_record::DynamicRecord>> {
        let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

        let mut buffer = Vec::new();
        let mut file = fs::File::open(path).unwrap();
        file.read_to_end(&mut buffer)?;

        match bincode::deserialize::<Vec<dynamic_record::DynamicRecord>>(&buffer[..]) {
            Ok(mut current_data_page_records) => {
                dbg!(&current_data_page_records);
                if selected_columns.is_some() {
                    for record in current_data_page_records.iter_mut() {
                        record.filter_columns(selected_columns.unwrap());
                    }
                }

                records.extend(current_data_page_records);
            }
            Err(e) => {
                eprintln!("Error deserializing records: {:?}", e);
            }
        };

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
        let table_name = "test_can_write_metadata_to_disk";

        engine.create_table(
            table_name.to_string(),
            vec![
                vec!["name".to_string(), "VARCHAR".to_string()],
                vec!["email".to_string(), "VARCHAR".to_string()],
            ],
        );

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
        let table_name = "test_can_insert_into_table";

        engine.create_table(
            table_name.to_string(),
            vec![
                vec!["name".to_string(), "VARCHAR".to_string()],
                vec!["email".to_string(), "VARCHAR".to_string()],
            ],
        );

        engine.insert(
            table_name.to_string(),
            vec!["name".to_string(), "email".to_string()],
            vec![
                vec!["john".to_string(), "john@mail.com".to_string()],
                vec!["doe".to_string(), "doe@mail.com".to_string()],
            ],
        );
    }

    #[test]
    #[should_panic]
    fn test_cannot_insert_invalid_type_into_table() {
        let engine = BinaryEngine::new();
        let table_name = "test_cannot_insert_invalid_type_into_table";

        engine.create_table(
            table_name.to_string(),
            vec![vec!["number".to_string(), "integer".to_string()]],
        );

        engine.insert(
            table_name.to_string(),
            vec!["number".to_string()],
            vec![vec!["john".to_string()]],
        );
    }

    #[test]
    fn test_can_select_from_table() {
        let engine = BinaryEngine::new();
        let table_name = "test_can_select_from_table";

        let database_base_dir =
            std::env::var("DATABASE_BASE_DIR").expect("DATABASE_BASE_DIR must be set");
        fs::remove_file(format!(
            "{}/{}/data_page_1.bin",
            database_base_dir, table_name
        ))
        .unwrap();

        engine.create_table(
            table_name.to_string(),
            vec![
                vec!["name".to_string(), "VARCHAR".to_string()],
                vec!["email".to_string(), "VARCHAR".to_string()],
            ],
        );

        engine.insert(
            table_name.to_string(),
            vec!["name".to_string(), "email".to_string()],
            vec![
                vec!["john".to_string(), "john@mail.com".to_string()],
                vec!["doe".to_string(), "doe@mail.com".to_string()],
            ],
        );

        let result = engine.select(table_name.to_string(), vec![String::from("name")]);

        match result {
            EngineResponse::Success { records, .. } => {
                let records = records.unwrap();
                assert_eq!(records.len(), 2);
                assert!(records.first().unwrap().fields.contains_key("name"));
                assert!(!records.first().unwrap().fields.contains_key("email"));
            }
            EngineResponse::Error { message } => panic!("{}", message),
        }
    }
}
