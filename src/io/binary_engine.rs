use super::Engine;
use crate::metadata::{self, Table};
use crate::query::{Query, Statement};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use serde_json;
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
        println!("Writing in binary..");
        println!("Executing query: '{}'", query.statement);

        let result = match &query.statement {
            Statement::CreateTable {
                table_name,
                columns,
            } => self.create_table(table_name.clone(), columns.clone()),
            Statement::Select {
                table_name,
                selection,
            } => self.select(query),
            Statement::Insert {
                table_name,
                columns,
                values,
            } => self.insert(query),
        };

        if result.is_err() {
            println!("The following error occured when trying to write the data..");
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
            fs::create_dir(table_path.clone()).expect("Failed to create dir for new table.");
        }

        let table = metadata::Table::new(table_name, columns);
        self.store_meta_data(&table)
            .expect("Failed to store meta-data.");

        fs::File::create(table_path + "/data_page_1.bin")?;

        Ok(())
    }

    fn store_meta_data(&self, table: &Table) -> io::Result<()> {
        let path = format!("{}/{}/metadata.bin", self.base_path, table.name);

        let serialized_table = &bincode::serialize(table).unwrap();

        let mut file = fs::File::create(path)?;
        file.write_all(serialized_table)?;

        Ok(())
    }

    fn load_meta_data(&self, table_name: String) -> io::Result<Table> {
        let path = format!("{}/{}/metadata.bin", self.base_path, table_name);

        let mut file = fs::File::open(path)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let table: Table = bincode::deserialize(&buffer[..]).unwrap();

        Ok(table)
    }

    fn insert(&self, query: Query) -> io::Result<()> {
        // insert data into file
        //     let record = DynamicRecord {
        //         id: 1,
        //         data: serde_json::json!({
        //             "name": "John Doe",
        //             "age": 30,
        //             "phones": [
        //                 "+44 1234567",
        //                 "+44 2345678"
        //             ]
        //         }),
        //     };
        // fn save_record(record: &DynamicRecord, filename: &str) -> std::io::Result<()> {
        //     let serialized = bincode::serialize(record).unwrap();
        //     let mut file = File::create(filename)?;
        //     file.write_all(&serialized)?;
        //     Ok(())
        // }
        Ok(())
    }

    fn select(&self, query: Query) -> io::Result<()> {
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
