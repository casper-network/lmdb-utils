use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::BlockHeaderV1;

use super::{Database, DeserializationError};

pub struct LegacyBlockHeaderDatabase;

impl Display for LegacyBlockHeaderDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_header")
    }
}

impl Database for LegacyBlockHeaderDatabase {
    fn db_name() -> &'static str {
        "block_header"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockHeaderV1 = bincode::deserialize(bytes)?;
        Ok(())
    }
}
