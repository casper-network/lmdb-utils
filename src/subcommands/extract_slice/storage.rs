use std::{collections::BTreeSet, fs, io::ErrorKind, path::Path, result::Result};

use casper_storage::block_store::{
    lmdb::LmdbBlockStore,
    types::{
        ApprovalsHashes, BlockExecutionResults, BlockHashHeightAndEra, BlockTransfers,
        ExecutionResults, TransactionFinalizedApprovals,
    },
    BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
};

use casper_types::{
    execution::ExecutionResult, Approval, Block, BlockHash, BlockSignatures, Digest, Transaction,
    Transfer,
};
use log::{info, warn};

use crate::common::db::{
    DEFAULT_MAX_BLOCK_STORE_SIZE, DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    DEFAULT_MAX_DEPLOY_STORE_SIZE, STORAGE_FILE_NAME,
};

use super::Error;

pub(crate) fn create_output_db_dir<P: AsRef<Path>>(output_path: P) -> Result<(), Error> {
    if output_path.as_ref().exists() {
        return Err(Error::Output(ErrorKind::AlreadyExists.into()));
    }
    fs::create_dir_all(&output_path)?;

    Ok(())
}

/// Given a block hash, reads the information related to the associated block
/// (block header, block body, deploys, transfers, execution results) and
/// copies them over to a new database. Returns the state root hash associated
/// with the block.
pub(crate) fn transfer_block_info<P1: AsRef<Path>, P2: AsRef<Path>>(
    source: P1,
    destination: P2,
    block_hash: BlockHash,
) -> Result<Digest, Error> {
    let source_path = source.as_ref().join(STORAGE_FILE_NAME);
    let destination_path = destination.as_ref().join(STORAGE_FILE_NAME);

    info!(
        "Initiating block information transfer from {} to {} for block {block_hash}",
        source_path.to_string_lossy(),
        destination_path.to_string_lossy()
    );

    let source_store = LmdbBlockStore::new(
        source.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;
    let source_txn = source_store.checkout_ro()?;

    let mut destination_store = LmdbBlockStore::new(
        destination.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;
    let mut destination_txn = destination_store.checkout_rw()?;

    // Read the block header and body associated with the given block hash.
    let block: Block = source_txn
        .read(block_hash)?
        .ok_or(Error::MissingBlock(block_hash))?;
    let block_height = block.height();
    let block_era = block.era_id();
    let state_root_hash = *block.state_root_hash();

    let hash = destination_txn.write(&block)?;
    debug_assert!(hash == block_hash);
    info!("Successfully transferred block");

    let mut exec_results = ExecutionResults::new();

    // Copy over all the transactions in this block and construct the execution
    // results to be stored in the new database.
    for transaction_hash in block.all_transaction_hashes() {
        let transaction: Transaction = source_txn
            .read(transaction_hash)?
            .ok_or(Error::MissingTransaction(transaction_hash))?;
        let hash = destination_txn.write(&transaction)?;
        debug_assert!(hash == transaction_hash);

        let maybe_finalized_approvals: Option<BTreeSet<Approval>> =
            source_txn.read(transaction_hash)?;

        if let Some(finalized_approvals) = maybe_finalized_approvals {
            let transaction_approvals = TransactionFinalizedApprovals {
                transaction_hash,
                finalized_approvals,
            };

            let hash = destination_txn.write(&transaction_approvals)?;
            debug_assert!(hash == transaction_hash);
        } else {
            warn!("Missing approvals hashes for transaction {transaction_hash}");
        }

        let exec_result: ExecutionResult = source_txn
            .read(transaction_hash)?
            .ok_or(Error::MissingExecutionResult(transaction_hash))?;
        exec_results.insert(transaction_hash, exec_result);

        info!("Successfully transferred transaction and approvals for {transaction_hash}");
    }

    if exec_results.is_empty() {
        info!("No execution results found in the source DB for block {block_hash}");
    } else {
        let block_info = BlockHashHeightAndEra::new(block_hash, block_height, block_era);
        let block_execution_results = BlockExecutionResults {
            block_info,
            exec_results,
        };
        destination_txn.write(&block_execution_results)?;
        info!("Successfully transferred block execution results for block {block_hash}");
    }

    // Attempt to copy over all entries in the transfer database for the given
    // block hash. If we have no entry under the block hash, we move on.
    let maybe_transfers: Option<Vec<Transfer>> = source_txn.read(block_hash)?;
    if let Some(transfers) = maybe_transfers {
        let block_transfers = BlockTransfers {
            block_hash,
            transfers,
        };
        let hash = destination_txn.write(&block_transfers)?;
        debug_assert!(hash == block_hash);
        info!("Found transfers in the source DB for block {block_hash} and successfully transferred them");
    } else {
        info!("No transfers found in the source DB for block {block_hash}");
    }

    let maybe_signatures: Option<BlockSignatures> = source_txn.read(block_hash)?;
    if let Some(signatures) = maybe_signatures {
        let hash = destination_txn.write(&signatures)?;
        debug_assert!(hash == block_hash);
        info!("Found block signatures in the source DB for block {block_hash} and successfully transferred them");
    } else {
        info!("No block signatures found in the source DB for block {block_hash}");
    }

    let maybe_approvals_hashes: Option<ApprovalsHashes> = source_txn.read(block_hash)?;
    if let Some(approvals_hashes) = maybe_approvals_hashes {
        let hash = destination_txn.write(&approvals_hashes)?;
        debug_assert!(hash == block_hash);
        info!("Found block approvals hashes in the source DB for block {block_hash} and successfully transferred them");
    } else {
        info!("No block approvals hashes found in the source DB for block {block_hash}");
    }

    // Commit the transactions.
    source_txn.commit()?;
    destination_txn.commit()?;
    info!("Storage transfer complete");
    Ok(state_root_hash)
}
