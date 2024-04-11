use super::{
    query::{Query, QueryResult, StatementType},
    statement_parser::statement_parser_factory,
};

pub struct SqlParser();

impl SqlParser {
    pub fn parse_query(&self, input: String) -> QueryResult {
        let trimmed_input = input.trim();
        let raw_graphemes: Vec<&str> = trimmed_input.split(' ').collect();
        let mut graphemes: Vec<String> = Vec::with_capacity(raw_graphemes.len());

        for grapheme in raw_graphemes {
            let grapheme_with_spaces = grapheme
                .replace('(', " ( ")
                .replace(',', " , ")
                .replace(')', " ) ")
                .replace(';', " ; ");

            for grapheme_with_space in grapheme_with_spaces.split(' ').collect::<Vec<&str>>() {
                let trimmed_subgrapheme = grapheme_with_space.trim();

                if !trimmed_subgrapheme.is_empty() {
                    graphemes.push(trimmed_subgrapheme.to_string());
                }
            }
        }

        if graphemes.last().expect("Query is required.") != ";" {
            return Err(String::from("Your statement must end with a semicolon."));
        } else if graphemes.len() < 2 {
            return Err(String::from("Invalid query."));
        }

        let statement_type = StatementType::new(&graphemes[0], &graphemes[1]);

        if let StatementType::Invalid = statement_type {
            return Err(String::from(
                "Unimplemented Command. Please use 'SELECT', 'CREATE TABLE' or 'INSERT'",
            ));
        }

        let mut statement_parser = statement_parser_factory(statement_type);

        match statement_parser.parse_statement(graphemes) {
            Ok(statement) => Ok(Query::new(input, statement)),
            Err(message) => Err(message),
        }
    }
}
