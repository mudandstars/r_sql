use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(table_name: String, columns_vectors: Vec<Vec<String>>) -> Self {
        let mut columns: Vec<Column> = Vec::new();

        for column_vector in columns_vectors {
            columns.push(Column {
                name: column_vector[0].clone(),
                data_type: column_vector[1].clone(),
                nullable: false,
            })
        }

        Table {
            name: table_name,
            columns,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
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
        assert_eq!(table.columns.first().unwrap().data_type, "VARCHAR");

        assert_eq!(table.columns.last().unwrap().name, "email");
        assert_eq!(table.columns.last().unwrap().data_type, "VARCHAR");
    }
}
