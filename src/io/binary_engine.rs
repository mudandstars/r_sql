use super::Engine;

pub struct BinaryEngine();

impl Engine for BinaryEngine {
    fn execute(&self, query: crate::query::Query) {
        println!("Writing in binary..");
        println!("Executing query on table {}", query.table_name);
    }
}

// use serde::{Serialize, Deserialize};
// use serde_json::Value;
// use std::fs::File;
// use std::io::{self, Write, Read};

// #[derive(Serialize, Deserialize, Debug)]
// struct DynamicRecord {
//     id: u32,
//     data: Value, // Use `serde_json::Value` for dynamic data.
// }

// fn save_record(record: &DynamicRecord, filename: &str) -> io::Result<()> {
//     let serialized = bincode::serialize(record).unwrap();
//     let mut file = File::create(filename)?;
//     file.write_all(&serialized)?;
//     Ok(())
// }

// fn load_record(filename: &str) -> io::Result<DynamicRecord> {
//     let mut file = File::open(filename)?;
//     let mut data = Vec::new();
//     file.read_to_end(&mut data)?;
//     let record: DynamicRecord = bincode::deserialize(&data[..]).unwrap();
//     Ok(record)
// }

// fn main() -> io::Result<()> {
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

//     save_record(&record, "record.dat")?;

//     let loaded_record = load_record("record.dat")?;
//     println!("Loaded record: {:?}", loaded_record);

//     Ok(())
// }

// use serde::{Serialize, Deserialize};
// use bincode;
// use std::fs::File;
// use std::io::{self, Write};

// #[derive(Serialize, Deserialize)]
// struct Record {
//     // Your record fields
// }

// #[derive(Serialize, Deserialize)]
// struct Page {
//     records: Vec<Record>, // Assume this is your 'fixed-size' block, conceptually
// }

// fn serialize_page(page: &Page) -> io::Result<()> {
//     let serialized = bincode::serialize(&page).unwrap();
//     let mut file = File::create("page.dat")?;
//     file.write_all(&serialized)?;
//     Ok(())
// }

// // Deserialization and data retrieval functions would be similar in reverse.
