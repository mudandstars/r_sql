use std::fs;
use std::io::{self, Read, Write};
use std::rc::Rc;

use crate::{engine::file_paths::FilePaths, metadata};

pub struct TableManager {
    file_paths: Rc<FilePaths>,
}

impl TableManager {
    pub fn new(file_paths: Rc<FilePaths>) -> Self {
        TableManager { file_paths }
    }

    pub fn store_meta_data(&self, table: &metadata::Table) -> io::Result<()> {
        let serialized_table = &bincode::serialize(table).unwrap();

        let mut file = fs::File::create(self.file_paths.meta_data_path(&table.name))?;
        file.write_all(serialized_table)?;

        for index in &table.indices {
            fs::File::create(self.file_paths.index_path(&table.name, &index.name))?;
        }

        Ok(())
    }

    pub fn load_meta_data(&self, table_name: &str) -> io::Result<metadata::Table> {
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
}
