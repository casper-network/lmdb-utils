use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::BlockBodyV1;

use super::{Database, DeserializationError};

pub struct LegacyBlockBodyDatabase;

impl Display for LegacyBlockBodyDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_body")
    }
}

impl Database for LegacyBlockBodyDatabase {
    fn db_name() -> &'static str {
        "block_body"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockBodyV1 = bincode::deserialize(bytes)?;
        Ok(())
    }
}
