mod approvals_hashes_db;
mod block_body_db;
mod block_body_v2_db;
mod block_header_db;
mod block_header_v2;
mod block_metadata_db;
mod block_metadata_v2_db;
mod deploy_metadata_db;
mod deploys_db;
mod execution_results_db;
mod finalized_approvals_db;
mod state_store_db;
mod transactions_db;
mod transfer_db;
mod versioned_approvals_hashes_db;
mod versioned_finalized_approvals_db;
mod versioned_transfers_db;

#[cfg(test)]
mod tests;

pub use approvals_hashes_db::ApprovalsHashesDatabase;
pub use block_body_db::LegacyBlockBodyDatabase;
pub use block_body_v2_db::VersionedBlockBodyDatabase;
pub use block_header_db::LegacyBlockHeaderDatabase;
pub use block_header_v2::VersionedBlockHeaderDatabase;
pub use block_metadata_db::LegacyBlockMetadataDatabase;
pub use block_metadata_v2_db::VersionedBlockMetadataDatabase;
pub use deploy_metadata_db::LegacyDeployMetadataDatabase;
pub use deploys_db::DeployDatabase;
pub use execution_results_db::VersionedExecutionResultsDatabase;
pub use finalized_approvals_db::FinalizedApprovalsDatabase;
pub use state_store_db::StateStoreDatabase;
pub use transactions_db::TransactionsDatabase;
pub use transfer_db::TransferDatabase;
pub use versioned_approvals_hashes_db::VersionedApprovalsHashesDatabase;
pub use versioned_finalized_approvals_db::VersionedFinalizedApprovalsDatabase;
pub use versioned_transfers_db::VersionedTransfersDatabase;

use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    path::Path,
    result::Result,
};

use bincode::Error as BincodeError;
use lmdb::{Cursor, Environment, EnvironmentFlags, Error as LmdbError, RoCursor, Transaction};
use log::info;
use thiserror::Error;

use casper_types::bytesrepr::Error as BytesreprError;

pub const STORAGE_FILE_NAME: &str = "storage.lmdb";
pub const TRIE_STORE_FILE_NAME: &str = "data.lmdb";
const ENTRY_LOG_INTERVAL: usize = 100_000;
const MAX_DB_READERS: u32 = 100;

const GIB: usize = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_BLOCK_STORE_SIZE: usize = 450 * GIB;
pub(crate) const DEFAULT_MAX_DEPLOY_STORE_SIZE: usize = 300 * GIB;
pub(crate) const DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE: usize = 300 * GIB;

#[derive(Debug, Error)]
pub enum DeserializationError {
    #[error("failed parsing struct with bincode")]
    BincodeError(#[from] BincodeError),
    #[error("failed parsing struct with bytesrepr")]
    BytesreprError(String),
}

impl From<BytesreprError> for DeserializationError {
    fn from(error: BytesreprError) -> Self {
        Self::BytesreprError(error.to_string())
    }
}

/// Errors encountered when operating on the storage database.
#[derive(Debug, Error)]
pub enum Error {
    /// Errors accumulated when parsing a database with "--no-failfast".
    Accumulated(Vec<Self>),
    /// Parsing error on entry at index in the database.
    Parsing(usize, DeserializationError),
    /// Database operation error.
    Database(#[from] LmdbError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        match self {
            Self::Database(e) => write!(f, "Error operating the database: {e}"),
            Self::Parsing(idx, inner) => write!(f, "Error parsing element {idx}: {inner}"),
            Self::Accumulated(accumulated_errors) => {
                writeln!(f, "Errors caught:")?;
                for error in accumulated_errors {
                    writeln!(f, "{error}")?;
                }
                Ok(())
            }
        }
    }
}

pub fn db_env<P: AsRef<Path>>(path: P) -> Result<Environment, LmdbError> {
    let env = Environment::new()
        .set_flags(
            EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::NO_TLS
                | EnvironmentFlags::NO_READAHEAD,
        )
        .set_max_dbs(MAX_DB_READERS)
        .open(path.as_ref())?;
    Ok(env)
}

pub trait Database {
    fn db_name() -> &'static str;

    /// Parses a value of an entry in a database.
    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError>;

    /// Parses all elements of a database by trying to deserialize them sequentially.
    fn parse_elements(mut cursor: RoCursor, failfast: bool, start_at: usize) -> Result<(), Error> {
        if start_at > 0 {
            info!("Skipping {} entries.", start_at);
        }
        let mut error_buffer = vec![];
        for (idx, entry) in cursor.iter().skip(start_at).enumerate() {
            let (_raw_key, raw_val) = entry.map_err(Error::Database)?;
            if let Err(e) =
                Self::parse_element(raw_val).map_err(|parsing_err| Error::Parsing(idx, parsing_err))
            {
                if failfast {
                    return Err(e);
                } else {
                    error_buffer.push(e);
                }
            }
            if idx % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", idx);
            }
        }
        info!("Parsing complete.");
        if !failfast && !error_buffer.is_empty() {
            return Err(Error::Accumulated(error_buffer));
        }
        Ok(())
    }

    /// Validates the database by ensuring every value of an entry can be parsed.
    fn check_db(env: &Environment, failfast: bool, start_at: usize) -> Result<(), Error> {
        info!("Checking {} database.", Self::db_name());
        let txn = env.begin_ro_txn()?;
        let db = unsafe { txn.open_db(Some(Self::db_name()))? };

        if let Ok(cursor) = txn.open_ro_cursor(db) {
            Self::parse_elements(cursor, failfast, start_at)?;
        }
        Ok(())
    }
}
