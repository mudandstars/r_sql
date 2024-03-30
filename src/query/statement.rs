pub enum Statement {
    Select {
        table_name: String,
        selection: Vec<String>,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<String>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<Vec<String>>,
    },
}

impl Statement {
    pub fn as_string(&self) -> String {
        match self {
            Self::Select {
                table_name,
                selection,
            } => format!("SELECT {} FROM {};", selection.join(", "), table_name),
            Self::Insert {
                table_name,
                columns,
                values,
            } => format!(
                "INSERT INTO {}({}) VALUES {};",
                table_name,
                columns.join(", "),
                values.join(", ")
            ),
            Self::CreateTable {
                table_name,
                columns,
            } => {
                let mut column_strings: Vec<String> = Vec::new();

                for col in columns.iter() {
                    column_strings.push(col.join(" "));
                }

                format!(
                    "CREATE TABLE {}(\n{}\n);",
                    table_name,
                    column_strings.join(",\n")
                )
            }
        }
    }
}
