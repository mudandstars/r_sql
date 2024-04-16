use dotenvy::dotenv;

pub struct FilePaths {
    base_path: String,
}

impl FilePaths {
    pub fn new() -> Self {
        dotenv().ok();

        let base_path = std::env::var("DATABASE_BASE_DIR").expect("DATABASE_BASE_DIR must be set");

        FilePaths { base_path }
    }

    pub fn table_path(&self, table_name: &str) -> String {
        String::from(&self.base_path) + "/" + table_name
    }

    pub fn meta_data_path(&self, table_name: &str) -> String {
        format!("{}/{}/metadata.bin", self.base_path, table_name)
    }

    pub fn data_page(&self, table_name: &str, data_page_index: u32) -> String {
        format!(
            "{}/{}/data_page_{}.bin",
            self.base_path, table_name, data_page_index
        )
    }
}
