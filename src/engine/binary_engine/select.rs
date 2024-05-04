use std::collections::HashMap;

use crate::engine;

impl engine::Select for super::BinaryEngine {
    fn select(
        &self,
        table_name: String,
        column_names: Vec<String>,
        where_clauses: HashMap<String, String>,
    ) -> super::EngineResult {
        let table = self.table_manager.load_meta_data(&table_name);
        if table.is_err() {
            return Err(String::from("This table does not exist."));
        }
        let table = table.unwrap();

        let data_page_indices = table.data_page_indices(&where_clauses);

        if !table.all_columns_exist(column_names.clone()) {
            return Err(String::from(
                "Please choose only columns that exist on this table.",
            ));
        }

        let records = self.data_manager.load_table_contents(
            &table_name,
            column_names,
            where_clauses,
            data_page_indices,
        );

        match records {
            Ok(records) => Ok(super::EngineResponse {
                records: Some(records),
                table: None,
            }),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{CreateTable, Insert, Select};
    use crate::{engine::binary_engine::BinaryEngine, io_test_context::FileTestContext};

    #[test]
    fn test_can_select_from_table() {
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

        let result = engine.select(
            context.table_name().to_string(),
            vec![String::from("name")],
            HashMap::new(),
        );

        match result {
            Ok(response) => {
                let records = response.records.unwrap();
                assert_eq!(records.len(), 2);
                assert!(records.first().unwrap().fields.contains_key("name"));
                assert!(!records.first().unwrap().fields.contains_key("email"));
            }
            Err(message) => panic!("{}", message),
        }
    }

    #[test]
    fn test_cannot_select_from_table_that_does_not_exist() {
        let engine = BinaryEngine::new();

        let result = engine.select(
            String::from("non_existant_table"),
            vec![String::from("name")],
            HashMap::new(),
        );

        if result.is_ok() {
            panic!()
        }
    }

    #[test]
    fn test_cannot_select_columns_from_table_that_do_not_exist() {
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
                vec!["name".to_string()],
                vec![vec!["john".to_string()], vec!["doe".to_string()]],
            )
            .unwrap();

        let result = engine.select(
            context.table_name().to_string(),
            vec![String::from("email")],
            HashMap::new(),
        );

        if result.is_ok() {
            panic!()
        }
    }

    #[test]
    fn test_can_select_with_multiple_where_clauses() {
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
                    vec!["john".to_string(), "john".to_string()],
                    vec!["doe".to_string(), "john".to_string()],
                    vec!["martin".to_string(), "john".to_string()],
                    vec!["doe".to_string(), "john".to_string()],
                    vec!["some".to_string(), "some@mail.com".to_string()],
                ],
            )
            .unwrap();

        let mut where_clauses = HashMap::new();
        where_clauses.insert(String::from("name"), "some".to_string());
        where_clauses.insert(String::from("email"), "some@mail.com".to_string());

        let result = engine.select(context.table_name().to_string(), vec![], where_clauses);

        match result {
            Ok(response) => {
                let records = response.records.unwrap();
                assert_eq!(records.len(), 1);

                assert!(records
                    .first()
                    .unwrap()
                    .fields
                    .get("name")
                    .unwrap()
                    .fullfills("some"));

                assert!(records
                    .first()
                    .unwrap()
                    .fields
                    .get("email")
                    .unwrap()
                    .fullfills("some@mail.com"));
            }
            Err(_) => panic!(),
        }
    }

    #[test]
    fn test_can_select_with_where_clause() {
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
                vec!["name".to_string()],
                vec![
                    vec!["john".to_string()],
                    vec!["doe".to_string()],
                    vec!["martin".to_string()],
                    vec!["doe".to_string()],
                    vec!["some".to_string()],
                ],
            )
            .unwrap();

        let mut where_clauses = HashMap::new();
        where_clauses.insert(String::from("id"), "1".to_string());

        let result = engine.select(
            context.table_name().to_string(),
            vec![String::from("name")],
            where_clauses,
        );

        match result {
            Ok(response) => {
                let records = response.records.unwrap();
                assert_eq!(records.len(), 1);

                assert!(records
                    .first()
                    .unwrap()
                    .fields
                    .get("name")
                    .unwrap()
                    .fullfills("john"));
            }
            Err(_) => panic!(),
        }
    }
}
