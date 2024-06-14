use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, BlockHeader};

use super::{Database, DeserializationError};

pub struct VersionedBlockHeaderDatabase;

impl Display for VersionedBlockHeaderDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_header_v2")
    }
}

impl Database for VersionedBlockHeaderDatabase {
    fn db_name() -> &'static str {
        "block_header_v2"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockHeader = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
