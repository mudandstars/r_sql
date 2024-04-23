use super::dynamic_record;
use super::file_paths::FilePaths;
use super::Engine;
use crate::metadata::{self, Table};
use crate::sql_parser::query::{Query, Statement};
use std::collections;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path;
use std::path::Path;

pub struct BinaryEngine {
    file_paths: FilePaths,
}

impl Engine for BinaryEngine {
    fn execute(&self, query: Query) -> super::EngineResult {
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
        let file_paths = FilePaths::new();

        BinaryEngine { file_paths }
    }

    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> super::EngineResult {
        let table_path = self.file_paths.table_path(&table_name);

        if !path::Path::new(&table_path).exists() {
            fs::create_dir(table_path.clone()).expect("\tFailed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name, columns);

        self.store_meta_data(&table)
            .expect("\tFailed to store meta-data.");

        Ok(super::EngineResponse {
            table: Some(table),
            records: None,
        })
    }

    fn store_meta_data(&self, table: &metadata::Table) -> io::Result<()> {
        let serialized_table = &bincode::serialize(table).unwrap();

        let mut file = fs::File::create(self.file_paths.meta_data_path(&table.name))?;
        file.write_all(serialized_table)?;

        for index in &table.indices {
            fs::File::create(self.file_paths.index_path(&table.name, &index.name))?;
        }

        Ok(())
    }

    fn load_meta_data(&self, table_name: &str) -> io::Result<metadata::Table> {
        let mut file = fs::File::open(self.file_paths.meta_data_path(table_name))?;

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
    ) -> super::EngineResult {
        let metadata = self.load_meta_data(&table_name);

        if metadata.is_err() {
            return Err(format!("Table '{}' does not exist.", table_name));
        }

        let mut metadata = metadata.unwrap();

        for value_vec in values {
            let mut dynamic_data = collections::HashMap::new();

            for (index, column_name) in column_names.iter().enumerate() {
                for metadata_column in &metadata.columns {
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
                            return Err(format!("Type does not allow {} value", value_vec[index]));
                        }
                    }
                }
            }

            if !dynamic_data.contains_key(&metadata.primary_key.name) {
                dynamic_data.insert(
                    metadata.primary_key.clone().name,
                    dynamic_record::Value::Text(metadata.new_primary_key().to_string()),
                );
            }

            let record = dynamic_record::DynamicRecord::new(dynamic_data);

            let result = self.save_record(record.clone(), &table_name);

            if let Err(err) = &result {
                return Err(err.to_string());
            }

            for index in metadata.indices.iter_mut() {
                index.update_tree((
                    record
                        .fields
                        .get(index.column_name.as_str())
                        .unwrap()
                        .to_string(),
                    result.as_ref().unwrap().clone(),
                ));
            }
        }

        self.store_meta_data(&metadata).unwrap();

        Ok(super::EngineResponse {
            table: None,
            records: None,
        })
    }

    fn save_record(
        &self,
        record: dynamic_record::DynamicRecord,
        table_name: &str,
    ) -> std::io::Result<usize> {
        let mut file_path;
        let mut data_page_index: usize = 1;

        loop {
            file_path = self.file_paths.data_page(table_name, data_page_index);

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

        Ok(data_page_index)
    }

    fn select(&self, table_name: String, column_names: Vec<String>) -> super::EngineResult {
        let table = self.load_meta_data(&table_name);
        if table.is_err() {
            return Err(String::from("This table does not exist."));
        }

        if !self.all_column_names_exist_on_table(table.unwrap(), column_names.clone()) {
            return Err(String::from(
                "Please choose only columns that exist on this table.",
            ));
        }

        let records = self.load_table_contents(&table_name, column_names);

        match records {
            Ok(records) => Ok(super::EngineResponse {
                records: Some(records),
                table: None,
            }),
            Err(e) => Err(e.to_string()),
        }
    }

    fn all_column_names_exist_on_table(&self, table: Table, column_names: Vec<String>) -> bool {
        let actual_table_columns: Vec<String> =
            table.columns.into_iter().map(|val| val.name).collect();

        let all_included = column_names
            .iter()
            .all(|item| actual_table_columns.contains(item));

        all_included
    }

    fn load_table_contents(
        &self,
        table_name: &str,
        column_names: Vec<String>,
    ) -> io::Result<Vec<dynamic_record::DynamicRecord>> {
        let mut data_page_index = 1;
        let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

        loop {
            let path = self.file_paths.data_page(table_name, data_page_index);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_test_context::FileTestContext;

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

        let table = engine.load_meta_data(context.table_name()).unwrap();
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

        let table = engine.load_meta_data(context.table_name()).unwrap();

        assert_eq!(table.primary_key.name, "id");
        match table.primary_key.data_type {
            metadata::SqlType::Integer => {}
            _ => panic!("failed"),
        }
        assert!(!table.primary_key.nullable);
    }

    #[test]
    fn test_updates_primary_key_index_on_inserts() {
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
                vec!["name".to_string(), "email".to_string()],
                vec![
                    vec!["john".to_string(), "john@mail.com".to_string()],
                    vec!["doe".to_string(), "doe@mail.com".to_string()],
                ],
            )
            .unwrap();

        let table = engine.load_meta_data(context.table_name()).unwrap();

        assert!(table.indices.first().unwrap().data_page("2").is_ok());
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

        let table = engine.load_meta_data(context.table_name()).unwrap();

        assert_eq!(table.indices.len(), 1);
        assert_eq!(table.indices.first().unwrap().column_name, "id");
    }

    #[test]
    fn test_can_insert_into_table() {
        let context = FileTestContext::new();

        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec!["email".to_string(), "VARCHAR".to_string()],
                ],
            )
            .unwrap();

        engine
            .insert(
                context.table_name().to_string(),
                vec!["name".to_string(), "email".to_string()],
                vec![
                    vec!["john".to_string(), "john@mail.com".to_string()],
                    vec!["doe".to_string(), "doe@mail.com".to_string()],
                ],
            )
            .unwrap();
    }

    #[test]
    fn test_cannot_insert_invalid_type_into_table() {
        let context = FileTestContext::new();

        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![vec!["number".to_string(), "integer".to_string()]],
            )
            .unwrap();

        if engine
            .insert(
                context.table_name().to_string(),
                vec!["number".to_string()],
                vec![vec!["john".to_string()]],
            )
            .is_ok()
        {
            panic!()
        }
    }

    #[test]
    fn test_can_select_from_table() {
        let context = FileTestContext::new();
        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![
                    vec!["name".to_string(), "VARCHAR".to_string()],
                    vec!["email".to_string(), "VARCHAR".to_string()],
                ],
            )
            .unwrap();

        engine
            .insert(
                context.table_name().to_string(),
                vec!["name".to_string(), "email".to_string()],
                vec![
                    vec!["john".to_string(), "john@mail.com".to_string()],
                    vec!["doe".to_string(), "doe@mail.com".to_string()],
                ],
            )
            .unwrap();

        let result = engine.select(context.table_name().to_string(), vec![String::from("name")]);

        match result {
            Ok(response) => {
                let records = response.records.unwrap();
                assert_eq!(records.len(), 2);
                assert!(records.first().unwrap().fields.contains_key("name"));
                assert!(!records.first().unwrap().fields.contains_key("email"));
            }
            Err(message) => panic!("{}", message),
        }
    }

    #[test]
    fn test_cannot_select_from_table_that_does_not_exist() {
        let engine = BinaryEngine::new();

        let result = engine.select(
            String::from("non_existant_table"),
            vec![String::from("name")],
        );

        if result.is_ok() {
            panic!()
        }
    }

    #[test]
    fn test_cannot_select_columns_from_table_that_do_not_exist() {
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

        let result = engine.select(
            context.table_name().to_string(),
            vec![String::from("email")],
        );

        if result.is_ok() {
            panic!()
        }
    }
}
