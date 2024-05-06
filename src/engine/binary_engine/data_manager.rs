use crate::engine::file_paths::FilePaths;

use super::dynamic_record;
use super::utils::selected_all_columns;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path;
use std::path::Path;
use std::rc::Rc;

pub struct DataManager {
    file_paths: Rc<FilePaths>,
}

impl DataManager {
    pub fn new(file_paths: Rc<FilePaths>) -> Self {
        DataManager { file_paths }
    }

    pub fn save_record(
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
            existing_contents = self.load_records(file_path.as_str(), None, None).unwrap();
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

    pub fn load_table_contents(
        &self,
        table_name: &str,
        column_names: Vec<String>,
        where_clauses: HashMap<String, String>,
        data_page_indices: Option<Vec<usize>>,
    ) -> io::Result<Vec<dynamic_record::DynamicRecord>> {
        let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

        match data_page_indices {
            Some(indices) => {
                for index in indices {
                    let path = self.file_paths.data_page(table_name, index);

                    records.extend(
                        self.load_records(&path, Some(&column_names), Some(&where_clauses))
                            .unwrap(),
                    );
                }

                Ok(records)
            }
            None => {
                let mut data_page_index = 1;
                let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

                loop {
                    let path = self.file_paths.data_page(table_name, data_page_index);

                    if !path::Path::new(&path).exists() {
                        break;
                    }

                    records.extend(
                        self.load_records(&path, Some(&column_names), Some(&where_clauses))
                            .unwrap(),
                    );

                    data_page_index += 1;
                }

                Ok(records)
            }
        }
    }

    fn load_records(
        &self,
        path: &str,
        selected_columns: Option<&Vec<String>>,
        where_clauses: Option<&HashMap<String, String>>,
    ) -> io::Result<Vec<dynamic_record::DynamicRecord>> {
        let mut records: Vec<dynamic_record::DynamicRecord> = vec![];

        let mut buffer = Vec::new();
        let mut file = fs::File::open(path).unwrap();
        file.read_to_end(&mut buffer)?;

        match bincode::deserialize::<Vec<dynamic_record::DynamicRecord>>(&buffer[..]) {
            Ok(mut current_data_page_records) => {
                if where_clauses.is_some() {
                    current_data_page_records
                        .retain(|record| record.entry_should_be_included(&where_clauses));
                }

                if selected_columns.is_some()
                    && !selected_columns.unwrap().is_empty()
                    && !selected_all_columns(selected_columns.unwrap())
                {
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
