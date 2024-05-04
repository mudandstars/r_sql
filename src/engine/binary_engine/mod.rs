mod create_index;
mod create_table;
mod data_manager;
mod insert;
mod select;
mod table_manager;

use std::rc::Rc;

use self::data_manager::DataManager;
use self::table_manager::TableManager;

use super::Engine;
use super::EngineResponse;
use super::EngineResult;

pub struct BinaryEngine {
    file_paths: Rc<super::file_paths::FilePaths>,
    data_manager: DataManager,
    table_manager: TableManager,
}

impl BinaryEngine {
    pub fn new() -> Self {
        let file_paths = Rc::new(FilePaths::new());
        let table_manager = TableManager::new(Rc::clone(&file_paths));
        let data_manager = DataManager::new(Rc::clone(&file_paths));

        BinaryEngine {
            file_paths,
            table_manager,
            data_manager,
        }
    }
}

impl Engine for BinaryEngine {}

use super::dynamic_record;
use super::file_paths::FilePaths;
