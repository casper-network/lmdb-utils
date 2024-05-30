use std::{
    collections::BTreeSet,
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, Approval};

use super::{Database, DeserializationError};

pub struct VersionedFinalizedApprovalsDatabase;

impl Display for VersionedFinalizedApprovalsDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "versioned_finalized_approvals")
    }
}

impl Database for VersionedFinalizedApprovalsDatabase {
    fn db_name() -> &'static str {
        "versioned_finalized_approvals"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BTreeSet<Approval> = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
