use std::{collections::HashMap, time::Instant};
mod identifier;

use crate::{dynamic_record, engine::Engine};

use self::identifier::Identifier;

pub struct WAL {
    entries: Vec<Entry>,
    engine: dyn Engine,
}

impl WAL {
    pub fn append_entry(
        &mut self,
        identifier: Identifier,
        changes: Vec<dynamic_record::DynamicRecord>,
    ) -> Result<(), String> {
        let entry = self.new_entry(identifier, changes);

        match entry {
            Ok(entry) => {
                self.entries.push(entry);
                Ok(())
            }
            Err(message) => Err(message),
        }
    }

    fn new_entry(
        &self,
        identifier: Identifier,
        changes: Vec<dynamic_record::DynamicRecord>,
    ) -> Result<Entry, String> {
        let mut where_clauses = HashMap::new();
        where_clauses.insert(identifier.column_name, identifier.column_value.to_string());
        let column_names = changes
            .first()
            .unwrap()
            .fields
            .keys()
            .cloned()
            .collect::<Vec<String>>();

        let engine_result = self
            .engine
            .select(identifier.table_name, column_names, where_clauses);

        match engine_result {
            Ok(response) => {
                let records = response.records.unwrap();

                Ok(Entry::new(response.table.unwrap().name, records, changes))
            }
            Err(message) => Err(message),
        }
    }
}

struct Entry {
    created_at: Instant,
    table_name: String,
    old_records: Vec<dynamic_record::DynamicRecord>,
    changes: Vec<dynamic_record::DynamicRecord>,
}

impl Entry {
    pub fn new(
        table_name: String,
        old_records: Vec<dynamic_record::DynamicRecord>,
        changes: Vec<dynamic_record::DynamicRecord>,
    ) -> Self {
        Self {
            created_at: Instant::now(),
            table_name,
            old_records,
            changes,
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_entry_can_be_properly_constructed() {

    }
}
