use std::path::Path;

use crate::common::db::{
    DEFAULT_MAX_BLOCK_STORE_SIZE, DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    DEFAULT_MAX_DEPLOY_STORE_SIZE,
};

use super::Error;
use casper_storage::block_store::{
    lmdb::LmdbBlockStore,
    types::{ApprovalsHashes, BlockExecutionResults, BlockHashHeightAndEra, BlockTransfers},
    BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
};
use casper_types::{Block, BlockHash, BlockHeader, BlockSignatures, Transaction, TransactionHash};

pub(crate) fn remove_block<P: AsRef<Path>>(db_path: P, block_hash: BlockHash) -> Result<(), Error> {
    let mut block_store = LmdbBlockStore::new(
        db_path.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;

    let mut rw_txn = block_store.checkout_rw()?;

    let maybe_block_header: Option<BlockHeader> = rw_txn.read(block_hash)?;
    let block_info = if let Some(header) = maybe_block_header {
        BlockHashHeightAndEra::new(block_hash, header.height(), header.era_id())
    } else {
        return Err(Error::MissingHeader(block_hash));
    };

    let maybe_block: Option<Block> = rw_txn.read(block_hash)?;
    if let Some(block) = maybe_block {
        for transaction_hash in block.all_transaction_hashes() {
            DataWriter::<TransactionHash, Transaction>::delete(&mut rw_txn, transaction_hash)?;
        }
    }

    DataWriter::<BlockHashHeightAndEra, BlockExecutionResults>::delete(&mut rw_txn, block_info)?;
    DataWriter::<BlockHash, BlockTransfers>::delete(&mut rw_txn, block_hash)?;
    DataWriter::<BlockHash, BlockSignatures>::delete(&mut rw_txn, block_hash)?;
    DataWriter::<BlockHash, ApprovalsHashes>::delete(&mut rw_txn, block_hash)?;
    DataWriter::<BlockHash, Block>::delete(&mut rw_txn, block_hash)?;

    rw_txn.commit()?;
    Ok(())
}
