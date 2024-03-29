use super::parsed_input::ParsedInput;

pub struct InputParser {
    parsed_input: ParsedInput,
}

impl InputParser {
    pub fn new(input: String) -> Self {
        InputParser {
            parsed_input: ParsedInput::new(input),
        }
    }

    pub fn execute(self) {
        self.parsed_input.execute();
    }
}
