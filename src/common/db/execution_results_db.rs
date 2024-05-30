use casper_types::{bytesrepr::FromBytes, execution::ExecutionResult};
use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use super::{Database, DeserializationError};

pub struct VersionedExecutionResultsDatabase;

impl Display for VersionedExecutionResultsDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "execution_results")
    }
}

impl Database for VersionedExecutionResultsDatabase {
    fn db_name() -> &'static str {
        "execution_results"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: ExecutionResult = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
