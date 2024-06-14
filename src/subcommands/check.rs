use std::path::{Path, PathBuf};

use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use thiserror::Error as ThisError;

use crate::common::db::{
    db_env, ApprovalsHashesDatabase, Database, DeployDatabase, Error as DbError,
    FinalizedApprovalsDatabase, LegacyBlockBodyDatabase, LegacyBlockHeaderDatabase,
    LegacyBlockMetadataDatabase, LegacyDeployMetadataDatabase, StateStoreDatabase,
    TransactionsDatabase, TransferDatabase, VersionedApprovalsHashesDatabase,
    VersionedBlockBodyDatabase, VersionedBlockHeaderDatabase, VersionedBlockMetadataDatabase,
    VersionedExecutionResultsDatabase, VersionedFinalizedApprovalsDatabase,
    VersionedTransfersDatabase, STORAGE_FILE_NAME,
};

pub const COMMAND_NAME: &str = "check";
const DB_PATH: &str = "db-path";
const NO_FAILFAST: &str = "no-failfast";
const SPECIFIC: &str = "specific";
const START_AT: &str = "start-at";

enum DisplayOrder {
    NoFailfast,
    DbPath,
    Specific,
    StartAt,
}

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Error checking the database: {0}")]
    Database(#[from] DbError),
    #[error("Error initializing lmdb environment at {0}: {1}")]
    Path(PathBuf, LmdbError),
    #[error("Unknown database {0}")]
    UnknownDb(String),
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .about(
            "Checks validity of entries in a storage database through ensuring deserialization is \
            successful.",
        )
        .display_order(display_order)
        .arg(
            Arg::new(NO_FAILFAST)
                .display_order(DisplayOrder::NoFailfast as usize)
                .short('f')
                .long(NO_FAILFAST)
                .takes_value(false)
                .help(
                    "Program will not terminate when failing to parse an element in the database.",
                ),
        )
        .arg(
            Arg::new(DB_PATH)
                .display_order(DisplayOrder::DbPath as usize)
                .required(true)
                .short('d')
                .long(DB_PATH)
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path of the directory with the `storage.lmdb` file."),
        )
        .arg(
            Arg::new(SPECIFIC)
                .display_order(DisplayOrder::Specific as usize)
                .short('s')
                .long(SPECIFIC)
                .takes_value(true)
                .value_name("DB_NAME")
                .help("Parse a specific database."),
        )
        .arg(
            Arg::new(START_AT)
                .display_order(DisplayOrder::StartAt as usize)
                .short('i')
                .long(START_AT)
                .takes_value(true)
                .value_name("ENTRY_INDEX")
                .requires(SPECIFIC)
                .default_value("0")
                .help(
                    "Entry index from which parsing will start. Requires \"--specific\" parameter \
                    to be set.",
                ),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = matches.value_of(DB_PATH).unwrap();
    let failfast = !matches.is_present(NO_FAILFAST);
    let specific = matches.value_of(SPECIFIC);
    let start_at: usize = matches
        .value_of(START_AT)
        .expect("should have a default")
        .parse()
        .unwrap_or_else(|_| panic!("Value of \"--{START_AT}\" must be an integer."));

    check_db(path, failfast, specific, start_at)
}

fn check_db<P: AsRef<Path>>(
    path: P,
    failfast: bool,
    specific: Option<&str>,
    start_at: usize,
) -> Result<(), Error> {
    let storage_path = path.as_ref().join(STORAGE_FILE_NAME);
    let env = db_env(storage_path)
        .map_err(|lmdb_err| Error::Path(path.as_ref().to_path_buf(), lmdb_err))?;
    if let Some(db_name) = specific {
        match db_name.trim() {
            "block_body" => LegacyBlockBodyDatabase::check_db(&env, failfast, start_at)?,
            "block_body_v2" => VersionedBlockBodyDatabase::check_db(&env, failfast, start_at)?,
            "block_header" => LegacyBlockHeaderDatabase::check_db(&env, failfast, start_at)?,
            "block_header_v2" => VersionedBlockHeaderDatabase::check_db(&env, failfast, start_at)?,
            "block_metadata" => LegacyBlockMetadataDatabase::check_db(&env, failfast, start_at)?,
            "block_metadata_v2" => {
                VersionedBlockMetadataDatabase::check_db(&env, failfast, start_at)?
            }
            "deploy_metadata" => LegacyDeployMetadataDatabase::check_db(&env, failfast, start_at)?,
            "execution_results" => {
                VersionedExecutionResultsDatabase::check_db(&env, failfast, start_at)?
            }
            "deploys" => DeployDatabase::check_db(&env, failfast, start_at)?,
            "transactions" => TransactionsDatabase::check_db(&env, failfast, start_at)?,
            "finalized_approvals" => {
                FinalizedApprovalsDatabase::check_db(&env, failfast, start_at)?
            }
            "versioned_finalized_approvals" => {
                VersionedFinalizedApprovalsDatabase::check_db(&env, failfast, start_at)?
            }
            "transfer" => TransferDatabase::check_db(&env, failfast, start_at)?,
            "versioned_transfers" => {
                VersionedTransfersDatabase::check_db(&env, failfast, start_at)?
            }
            "state_store" => StateStoreDatabase::check_db(&env, failfast, start_at)?,
            "approvals_hashes" => ApprovalsHashesDatabase::check_db(&env, failfast, start_at)?,
            "versioned_approvals_hashes" => {
                VersionedApprovalsHashesDatabase::check_db(&env, failfast, start_at)?
            }
            _ => return Err(Error::UnknownDb(db_name.to_string())),
        }
    } else {
        // Sanity check for `start_at`, already validated in arg parser.
        assert_eq!(start_at, 0);
        LegacyBlockBodyDatabase::check_db(&env, failfast, start_at)?;
        VersionedBlockBodyDatabase::check_db(&env, failfast, start_at)?;
        LegacyBlockHeaderDatabase::check_db(&env, failfast, start_at)?;
        VersionedBlockHeaderDatabase::check_db(&env, failfast, start_at)?;
        LegacyBlockMetadataDatabase::check_db(&env, failfast, start_at)?;
        VersionedBlockMetadataDatabase::check_db(&env, failfast, start_at)?;
        LegacyDeployMetadataDatabase::check_db(&env, failfast, start_at)?;
        VersionedExecutionResultsDatabase::check_db(&env, failfast, start_at)?;
        DeployDatabase::check_db(&env, failfast, start_at)?;
        TransactionsDatabase::check_db(&env, failfast, start_at)?;
        FinalizedApprovalsDatabase::check_db(&env, failfast, start_at)?;
        VersionedFinalizedApprovalsDatabase::check_db(&env, failfast, start_at)?;
        TransferDatabase::check_db(&env, failfast, start_at)?;
        VersionedTransfersDatabase::check_db(&env, failfast, start_at)?;
        StateStoreDatabase::check_db(&env, failfast, start_at)?;
        ApprovalsHashesDatabase::check_db(&env, failfast, start_at)?;
        VersionedApprovalsHashesDatabase::check_db(&env, failfast, start_at)?;
    };
    Ok(())
}
