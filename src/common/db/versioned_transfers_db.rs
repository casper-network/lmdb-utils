use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, Transfer};

use super::{Database, DeserializationError};

pub struct VersionedTransfersDatabase;

impl Display for VersionedTransfersDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "versioned_transfers")
    }
}

impl Database for VersionedTransfersDatabase {
    fn db_name() -> &'static str {
        "versioned_transfers"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<Transfer> = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
