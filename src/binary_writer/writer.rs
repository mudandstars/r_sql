use crate::input_parser::InputParser;

pub struct BinaryWriter {
    input_parser: InputParser,
}

impl BinaryWriter {
    pub fn new(input_parser: InputParser) -> Self {
        BinaryWriter { input_parser }
    }

    pub fn write(self) {
        println!("Writing in binary..")
    }
}
