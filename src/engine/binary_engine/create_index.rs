use std::path;

use crate::{engine::create_index::CreateIndex, metadata};

impl CreateIndex for super::BinaryEngine {
    fn create_index(
        &self,
        table_name: String,
        column_name: String,
        index_name: String,
    ) -> super::EngineResult {
        let table_path = self.file_paths.table_path(&table_name);

        if !path::Path::new(&table_path).exists() {
            return Err(String::from("Table does not exist"));
        }

        let table = self.table_manager.load_meta_data(&table_name);

        match table {
            Ok(table) => {
                if table
                    .columns
                    .iter()
                    .filter(|column| column.name == column_name)
                    .collect::<Vec<&metadata::Column>>()
                    .is_empty()
                {
                    return Err(format!(
                        "'{}' does not exist on '{}'",
                        column_name, table_name
                    ));
                }

                if !table
                    .indices
                    .iter()
                    .filter(|index| index.column_name == column_name)
                    .collect::<Vec<&metadata::Index>>()
                    .is_empty()
                {
                    return Err(format!(
                        "'{}({})' is already indexed.",
                        table_name, column_name
                    ));
                }

                let mut table = table;
                table
                    .indices
                    .push(metadata::Index::new(index_name, &column_name));

                let result = self.table_manager.store_meta_data(&table);

                if result.is_err() {
                    return Err("Error storing new meta-data.".to_string());
                }

                Ok(super::EngineResponse {
                    table: Some(table),
                    records: None,
                })
            }
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{engine::binary_engine::BinaryEngine, io_test_context::FileTestContext};
    use crate::engine::{CreateTable, CreateIndex, Insert};

    #[test]
    fn test_can_set_indices_after_table_creation() {
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
            .create_index(
                context.table_name().to_string(),
                String::from("email"),
                String::from("email_index"),
            )
            .unwrap();

        let table = engine.table_manager.load_meta_data(context.table_name()).unwrap();

        assert_eq!(table.indices.len(), 2);
        assert_eq!(table.indices.last().unwrap().column_name, "email");
    }

    #[test]
    fn test_cannot_set_the_same_index_twice() {
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
            .create_index(
                context.table_name().to_string(),
                String::from("email"),
                String::from("email_index"),
            )
            .unwrap();

        match engine.create_index(
            context.table_name().to_string(),
            String::from("email"),
            String::from("email_index"),
        ) {
            Ok(..) => panic!(),
            Err(message) => assert_eq!(
                message,
                format!(
                    "'{}({})' is already indexed.",
                    context.table_name(),
                    "email"
                )
            ),
        }
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

        let table = engine.table_manager.load_meta_data(context.table_name()).unwrap();

        assert!(table
            .indices
            .first()
            .unwrap()
            .data_page_indices("2")
            .is_ok());
    }
}
