pub fn selected_all_columns(column_names: &Vec<String>) -> bool {
    column_names.len() == 1 && column_names.first().unwrap() == "*"
}
