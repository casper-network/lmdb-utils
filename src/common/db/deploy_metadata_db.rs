use casper_types::{execution::ExecutionResultV1, BlockHash};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use super::{Database, DeserializationError};

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct DeployMetadataV1 {
    execution_results: HashMap<BlockHash, ExecutionResultV1>,
}

pub struct LegacyDeployMetadataDatabase;

impl Display for LegacyDeployMetadataDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "deploy_metadata")
    }
}

impl Database for LegacyDeployMetadataDatabase {
    fn db_name() -> &'static str {
        "deploy_metadata"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: DeployMetadataV1 = bincode::deserialize(bytes)?;
        Ok(())
    }
}
