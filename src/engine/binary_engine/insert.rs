use std::collections::HashMap;

use crate::engine::{dynamic_record, insert::Insert};

impl Insert for super::BinaryEngine {
    fn insert(
        &self,
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> super::EngineResult {
        let metadata = self.table_manager.load_meta_data(&table_name);

        if metadata.is_err() {
            return Err(format!("Table '{}' does not exist.", table_name));
        }

        let mut metadata = metadata.unwrap();

        for value_vec in values {
            let mut dynamic_data = HashMap::new();

            for (index, column_name) in column_names.iter().enumerate() {
                for metadata_column in &metadata.columns {
                    if metadata_column.name == column_name.as_str() {
                        if metadata_column
                            .data_type
                            .allows_value(value_vec[index].clone())
                        {
                            dynamic_data.insert(
                                column_name.to_string(),
                                dynamic_record::Value::Text(value_vec[index].clone()),
                            );
                        } else {
                            return Err(format!("Type does not allow {} value", value_vec[index]));
                        }
                    }
                }
            }

            if !dynamic_data.contains_key(&metadata.primary_key.name) {
                dynamic_data.insert(
                    metadata.primary_key.clone().name,
                    dynamic_record::Value::Text(metadata.new_primary_key().to_string()),
                );
            }

            let record = dynamic_record::DynamicRecord::new(dynamic_data);

            let result = self.data_manager.save_record(record.clone(), &table_name);

            if let Err(err) = &result {
                return Err(err.to_string());
            }

            for index in metadata.indices.iter_mut() {
                index.update_tree((
                    record
                        .fields
                        .get(index.column_name.as_str())
                        .unwrap()
                        .to_string(),
                    result.as_ref().unwrap().clone(),
                ));
            }
        }

        self.table_manager.store_meta_data(&metadata).unwrap();

        Ok(super::EngineResponse {
            table: None,
            records: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{CreateTable, Insert};
    use crate::{engine::binary_engine::BinaryEngine, io_test_context::FileTestContext};

    #[test]
    fn test_can_insert_into_table() {
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
            .insert(
                context.table_name().to_string(),
                vec!["name".to_string(), "email".to_string()],
                vec![
                    vec!["john".to_string(), "john@mail.com".to_string()],
                    vec!["doe".to_string(), "doe@mail.com".to_string()],
                ],
            )
            .unwrap();
    }

    #[test]
    fn test_cannot_insert_invalid_type_into_table() {
        let context = FileTestContext::new();

        let engine = BinaryEngine::new();

        engine
            .create_table(
                context.table_name().to_string(),
                vec![vec!["number".to_string(), "integer".to_string()]],
            )
            .unwrap();

        if engine
            .insert(
                context.table_name().to_string(),
                vec!["number".to_string()],
                vec![vec!["john".to_string()]],
            )
            .is_ok()
        {
            panic!()
        }
    }
}
