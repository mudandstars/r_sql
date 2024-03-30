mod parser;
mod statement_parser;

use std::str::Split;

pub type QueryIterator<'a> = Split<'a, char>;

pub use crate::input_parser::parser::InputParser;
