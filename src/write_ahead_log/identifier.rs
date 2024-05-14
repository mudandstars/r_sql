pub struct Identifier {
    pub table_name: String,
    pub column_name: String,
    pub column_value: super::dynamic_record::Value,
}

impl Identifier {
    pub fn new(
        table_name: String,
        column_name: String,
        column_value: super::dynamic_record::Value,
    ) -> Self {
        Identifier {
            table_name,
            column_name,
            column_value,
        }
    }
}
