use super::Engine;
use crate::query::{Query, Statement};
// use serde::{Deserialize, Serialize};
// use serde_json;
// use std::{self, fs::File};

// #[derive(Serialize, Deserialize, Debug)]
// struct DynamicRecord {
//     data: serde_json::Value,
// }

pub struct BinaryEngine {
    base_path: String,
}

impl Engine for BinaryEngine {
    fn execute(&self, query: Query) {
        println!("Writing in binary..");
        println!("Executing query: '{}'", query.statement.as_string());

        match &query.statement {
            Statement::CreateTable {
                table_name,
                columns,
            } => self.create_table(query),
            Statement::Select {
                table_name,
                selection,
            } => self.select(query),
            Statement::Insert {
                table_name,
                columns,
                values,
            } => self.insert(query),
        }
    }
}

impl BinaryEngine {
    pub fn new(base_path: String) -> Self {
        BinaryEngine { base_path }
    }

    fn create_table(&self, query: Query) {
        // create the dir if not exists
        // create meta-data file
        // create first data-page file
        // let mut file = File::create(query.table_name);
    }

    fn insert(&self, query: Query) {
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
    }

    fn select(&self, query: Query) {
        // retrieve data from file
        // fn load_record(filename: &str) -> io::Result<DynamicRecord> {
        //     let mut file = File::open(filename)?;
        //     let mut data = Vec::new();
        //     file.read_to_end(&mut data)?;
        //     let record: DynamicRecord = bincode::deserialize(&data[..]).unwrap();
        //     Ok(record)
        // }
    }
}

impl Default for BinaryEngine {
    fn default() -> Self {
        BinaryEngine {
            base_path: String::from("../data"),
        }
    }
}
