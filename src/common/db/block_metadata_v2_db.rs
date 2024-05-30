use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, BlockSignatures};

use super::{Database, DeserializationError};

pub struct VersionedBlockMetadataDatabase;

impl Display for VersionedBlockMetadataDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_metadata_v2")
    }
}

impl Database for VersionedBlockMetadataDatabase {
    fn db_name() -> &'static str {
        "block_metadata_v2"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockSignatures = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
