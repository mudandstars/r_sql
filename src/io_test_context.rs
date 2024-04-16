use std::fs;

use rand::{distributions::Alphanumeric, Rng};

pub struct FileTestContext {
    table_name: String,
}

impl FileTestContext {
    pub fn new() -> Self {
        let table_name = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        FileTestContext { table_name }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl Drop for FileTestContext {
    fn drop(&mut self) {
        let database_base_dir =
            std::env::var("DATABASE_BASE_DIR").expect("DATABASE_BASE_DIR must be set");

        let table_dir = database_base_dir + "/" + &self.table_name;

        match fs::remove_dir_all(table_dir) {
            Ok(_) => println!("Removed dir successfully"),
            Err(err) => println!("Error cleaning up table dir: {}", err),
        };
    }
}
