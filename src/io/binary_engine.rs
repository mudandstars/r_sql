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

    fn create_table(&self, table_name: String, columns: Vec<Vec<String>>) -> io::Result<()> {
        let table_path = String::from(&self.base_path) + "/" + &table_name;

        if !path::Path::new(&table_path).exists() {
            fs::create_dir(table_path.clone()).expect("\tFailed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name.clone(), columns.clone());
        self.store_meta_data(&table)
            .expect("\tFailed to store meta-data.");

        fs::File::create(table_path + "/data_page_1.bin")?;

        Ok(())
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
    ) -> io::Result<()> {
        let metadata = self.load_meta_data(&table_name);
        // TODO metadata does not load properly

        if metadata.is_err() {
            super::raise_error(format!("Table '{}' does not exist.", table_name).as_str())
        }

        // for each value in values
        // for each column_names
        // if it is in metadata.column_names
        // add it to the json and set the value to the value of the index

        let metadata_columns = metadata.unwrap().columns;

        for value_vec in values {
            let mut dynamic_data = collections::HashMap::new();

            for (index, column_name) in column_names.iter().enumerate() {
                for metadata_column in &metadata_columns {
                    if metadata_column.name == column_name.as_str() {
                        //TODO implement check that column_names length must equal values length in respective parser
                        //TODO imlement type checks
                        dynamic_data
                            .insert(column_name.to_string(), serde_json::json!(value_vec[index]));
                    }
                }
            }

            let data = serde_json::Value::Object(dynamic_data.into_iter().collect());
            let record = DynamicRecord { data };

            self.save_record(&record, &table_name)?;
        }

        Ok(())
    }

    fn save_record(&self, record: &DynamicRecord, filename: &str) -> std::io::Result<()> {
        // TODO use data pages to handle this correctly
        if !path::Path::new(filename).exists() {
            fs::create_dir(filename)
                .expect("\tFailed to create new data page when attempting to store data.");
        }

        let serialized = bincode::serialize(record).unwrap();

        let mut file = fs::File::create(filename)?;
        file.write_all(&serialized)?;

        Ok(())
    }

    fn select(&self, table_name: String, column_names: Vec<String>) -> io::Result<()> {
        // retrieve data from file
        // fn load_record(filename: &str) -> io::Result<DynamicRecord> {
        //     let mut file = File::open(filename)?;
        //     let mut data = Vec::new();
        //     file.read_to_end(&mut data)?;
        //     let record: DynamicRecord = bincode::deserialize(&data[..]).unwrap();
        //     Ok(record)
        // }
        Ok(())
    }
}

impl Default for BinaryEngine {
    fn default() -> Self {
        BinaryEngine {
            base_path: String::from("/Users/paul/r_sql/"),
        }
    }
}
