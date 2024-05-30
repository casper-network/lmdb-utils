use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_storage::block_store::types::ApprovalsHashes;
use casper_types::bytesrepr::FromBytes;

use super::{Database, DeserializationError};

pub struct VersionedApprovalsHashesDatabase;

impl Display for VersionedApprovalsHashesDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "versioned_approvals_hashes")
    }
}

impl Database for VersionedApprovalsHashesDatabase {
    fn db_name() -> &'static str {
        "versioned_approvals_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: ApprovalsHashes = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
