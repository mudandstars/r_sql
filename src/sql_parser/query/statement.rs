use core::fmt;
use std::collections::HashMap;

pub enum Statement {
    Select {
        table_name: String,
        selection: Vec<String>,
        where_clauses: HashMap<String, String>,
    },
    InsertInto {
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Vec<String>>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<Vec<String>>,
    },
    CreateIndex {
        table_name: String,
        column_name: String,
        index_name: String,
    },
}

impl Statement {
    pub fn table_name(&self) -> &str {
        match self {
            Self::Select { table_name, .. }
            | Self::InsertInto { table_name, .. }
            | Self::CreateTable { table_name, .. }
            | Self::CreateIndex { table_name, .. } => table_name,
        }
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select {
                table_name,
                selection,
                where_clauses,
            } => write!(f, "SELECT {} FROM {};", selection.join(", "), table_name),
            Self::InsertInto {
                table_name,
                column_names: columns,
                values,
            } => {
                let mut values_strings: Vec<String> = Vec::new();

                for col in values.iter() {
                    values_strings.push(format!("(\n{}\n)", col.join(", ")));
                }
                write!(
                    f,
                    "INSERT INTO {}(\n{}\n) VALUES {};",
                    table_name,
                    columns.join(", "),
                    values_strings.join(", ")
                )
            }
            Self::CreateTable {
                table_name,
                columns,
            } => {
                let mut column_strings: Vec<String> = Vec::new();

                for col in columns.iter() {
                    column_strings.push(col.join(" "));
                }

                write!(
                    f,
                    "CREATE TABLE {}(\n{}\n);",
                    table_name,
                    column_strings.join(",\n")
                )
            }
            Self::CreateIndex {
                table_name,
                column_name,
                index_name,
            } => {
                write!(
                    f,
                    "CREATE INDEX {}\nON {}({});",
                    index_name, table_name, column_name
                )
            }
        }
    }
}
