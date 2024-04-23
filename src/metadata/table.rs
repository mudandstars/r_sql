use super::sql_type::SqlType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    pub name: String,
    pub primary_key: super::Column,
    latest_primary_key: usize,
    pub columns: Vec<super::Column>,
    pub indices: Vec<super::Index>,
}

impl Table {
    pub fn new(table_name: String, columns_vectors: Vec<Vec<String>>) -> Self {
        let mut columns: Vec<super::Column> = Vec::new();
        let mut primary_key = super::Column {
            name: String::from("id"),
            data_type: SqlType::Integer,
            nullable: false,
        };

        for column_vector in columns_vectors {
            if column_vector[1].to_uppercase() == "PRIMARY KEY" {
                primary_key.name = column_vector[0].clone();
                continue;
            }

            columns.push(super::Column {
                name: column_vector[0].clone(),
                data_type: SqlType::from(column_vector[1].clone()),
                nullable: false,
            })
        }

        Table {
            name: table_name,
            columns,
            indices: vec![super::Index::new(&primary_key.name)],
            primary_key,
            latest_primary_key: 0,
        }
    }

    pub fn new_primary_key(&mut self) -> usize {
        let new_key = self.latest_primary_key + 1;

        self.latest_primary_key = new_key;

        new_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_create_a_table_with_the_columns_input() {
        let table = Table::new(
            "my_table".to_string(),
            vec![
                vec![String::from("name"), String::from("VARCHAR")],
                vec![String::from("email"), String::from("VARCHAR")],
            ],
        );

        assert_eq!(table.columns.first().unwrap().name, "name");
        match table.columns.first().unwrap().data_type {
            SqlType::Varchar => {}
            _ => panic!("failed"),
        }

        assert_eq!(table.columns.last().unwrap().name, "email");

        match table.columns.last().unwrap().data_type {
            SqlType::Varchar => {}
            _ => panic!("failed"),
        }
    }
}
