use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, BlockBody};

use super::{Database, DeserializationError};

pub struct VersionedBlockBodyDatabase;

impl Display for VersionedBlockBodyDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_body_v2")
    }
}

impl Database for VersionedBlockBodyDatabase {
    fn db_name() -> &'static str {
        "block_body_v2"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockBody = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
