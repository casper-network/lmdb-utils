use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::BlockSignaturesV1;

use super::{Database, DeserializationError};

pub struct LegacyBlockMetadataDatabase;

impl Display for LegacyBlockMetadataDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_metadata")
    }
}

impl Database for LegacyBlockMetadataDatabase {
    fn db_name() -> &'static str {
        "block_metadata"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockSignaturesV1 = bincode::deserialize(bytes)?;
        Ok(())
    }
}
