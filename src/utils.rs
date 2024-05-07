use std::time::{Duration, Instant};

use crate::engine::EngineResponse;

pub fn log_engine_output(response: Result<EngineResponse, String>, duration: Duration) {
    match response {
        Ok(response) => {
            println!("Success ({} microseconds)", duration.as_micros());

            if response.table.is_some() {
                println!("{:?}", response.table.unwrap());
            }

            if response.records.is_some() {
                println!("{:?}", response.records.unwrap());
            }
        }
        Err(message) => println!("ERROR: {}", message),
    }
}

pub fn track_time<F, T>(func: F) -> (Duration, T)
where
    F: FnOnce() -> T,
{
    let start = Instant::now();

    let result = func();

    let duration = start.elapsed();

    (duration, result)
}
