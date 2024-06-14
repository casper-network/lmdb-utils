use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
};

use casper_storage::block_store::{
    lmdb::{IndexedLmdbBlockStore, LmdbBlockStore},
    types::{BlockHeight, Tip},
    BlockStoreProvider, DataReader,
};
use log::{info, warn};
use serde_json::{self, Error as JsonSerializationError};

use casper_types::{
    execution::ExecutionResult, Block, BlockHeader, ProtocolVersion, TransactionHash,
};

use crate::common::{
    db::{
        DEFAULT_MAX_BLOCK_STORE_SIZE, DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
        DEFAULT_MAX_DEPLOY_STORE_SIZE,
    },
    progress::ProgressTracker,
};

use super::{
    summary::{ExecutionResultsStats, ExecutionResultsSummary},
    Error,
};

fn get_execution_results_stats<P: AsRef<Path>>(
    db_path: P,
    log_progress: bool,
) -> Result<ExecutionResultsStats, Error> {
    let block_store = LmdbBlockStore::new(
        db_path.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;

    let indexed_block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::from_parts(0, 0, 0))?;
    let ro_txn = indexed_block_store.checkout_ro()?;

    let mut maybe_progress_tracker = None;
    let mut block_heights = vec![];
    let latest_block_header =
        DataReader::<Tip, BlockHeader>::read(&ro_txn, Tip)?.ok_or(Error::EmptyDatabase)?;
    let maybe_block_heights = 0..=latest_block_header.height();

    if log_progress {
        for height in maybe_block_heights {
            if DataReader::<BlockHeight, BlockHeader>::exists(&ro_txn, height)? {
                block_heights.push(height);
            }
        }

        match ProgressTracker::new(
            block_heights.len(),
            Box::new(|completion| info!("Database parsing {}% complete...", completion)),
        ) {
            Ok(progress_tracker) => maybe_progress_tracker = Some(progress_tracker),
            Err(progress_tracker_error) => warn!(
                "Couldn't initialize progress tracker: {}",
                progress_tracker_error
            ),
        }
    } else {
        block_heights = maybe_block_heights.collect();
    }

    let mut stats = ExecutionResultsStats::default();
    for block_height in block_heights {
        if let Some(block) = DataReader::<BlockHeight, Block>::read(&ro_txn, block_height)? {
            // Set of execution results of this block.
            let mut execution_results = vec![];
            // Go through all the transactions in this block and get the execution result of each one.
            for transaction_hash in block.all_transaction_hashes() {
                if let Some(exec_result) =
                    DataReader::<TransactionHash, ExecutionResult>::read(&ro_txn, transaction_hash)?
                {
                    execution_results.push(exec_result);
                }
            }
            // Update the statistics with this block's execution results.
            stats.feed(execution_results)?;

            if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
                progress_tracker.advance_by(1);
            }
        } else {
            continue;
        }
    }

    Ok(stats)
}

pub(crate) fn dump_execution_results_summary<W: Write + ?Sized>(
    summary: &ExecutionResultsSummary,
    out_writer: Box<W>,
) -> Result<(), JsonSerializationError> {
    serde_json::to_writer_pretty(out_writer, summary)
}

pub fn execution_results_summary<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: Option<P2>,
    overwrite: bool,
) -> Result<(), Error> {
    let mut log_progress = false;
    // Validate the output file early so that, in case this fails
    // we don't unnecessarily read the whole database.
    let out_writer: Box<dyn Write> = if let Some(out_path) = output {
        let file = OpenOptions::new()
            .create_new(!overwrite)
            .write(true)
            .open(out_path)?;
        log_progress = true;
        Box::new(file)
    } else {
        Box::new(io::stdout())
    };

    let execution_results_stats = get_execution_results_stats(&db_path, log_progress)?;
    let execution_results_summary: ExecutionResultsSummary = execution_results_stats.into();
    dump_execution_results_summary(&execution_results_summary, out_writer)?;

    Ok(())
}
