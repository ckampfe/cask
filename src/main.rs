use std::error::Error;

use cask::{Cask, Options};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct SomeData {
    a: usize,
    b: String,
    c: i8,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut options = Options::default();
    options.data_directory = "./data".to_string();
    let mut bc: Cask<String, SomeData> = Cask::open(options)?;

    dbg!(&bc);

    bc.write(
        "some key".to_string(),
        SomeData {
            a: 99,
            b: "foo".to_string(),
            c: -1,
        },
    )?;

    dbg!(&bc);

    let out = bc.read(&"some key".to_string())?;

    dbg!(out);

    dbg!(bc.keys()?);

    bc.delete("some key".to_string())?;

    dbg!(bc.keys()?);

    let out = bc.read(&"some key".to_string())?;

    dbg!(out);

    Ok(())
}
