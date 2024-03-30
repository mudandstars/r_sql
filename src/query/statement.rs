use core::fmt;

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
    pub fn table_name(&self) -> &str {
        match self {
            Self::Select { table_name, .. }
            | Self::Insert { table_name, .. }
            | Self::CreateTable { table_name, .. } => table_name,
        }
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select {
                table_name,
                selection,
            } => write!(f, "SELECT {} FROM {};", selection.join(", "), table_name),
            Self::Insert {
                table_name,
                columns,
                values,
            } => write!(
                f,
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

                write!(
                    f,
                    "CREATE TABLE {}(\n{}\n);",
                    table_name,
                    column_strings.join(",\n")
                )
            }
        }
    }
}
