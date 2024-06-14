use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
};

use casper_storage::block_store::{
    lmdb::{IndexedLmdbBlockStore, LmdbBlockStore},
    types::Tip,
    BlockStoreProvider, DataReader,
};
use casper_types::ProtocolVersion;
use log::warn;
use serde_json::{self, Error as SerializationError};

use casper_types::BlockHeader;

use crate::common::db::{
    DEFAULT_MAX_BLOCK_STORE_SIZE, DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    DEFAULT_MAX_DEPLOY_STORE_SIZE,
};

use super::{
    block_info::{parse_network_name, BlockInfo},
    Error,
};

fn get_highest_block<P: AsRef<Path>>(db_path: P) -> Result<BlockHeader, Error> {
    let block_store = LmdbBlockStore::new(
        db_path.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;

    let indexed_block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::from_parts(0, 0, 0))?;
    let ro_txn = indexed_block_store.checkout_ro()?;

    DataReader::<Tip, BlockHeader>::read(&ro_txn, Tip)?.ok_or(Error::EmptyDatabase)
}

pub(crate) fn dump_block_info<W: Write + ?Sized>(
    block_header: &BlockInfo,
    out_writer: Box<W>,
) -> Result<(), SerializationError> {
    serde_json::to_writer_pretty(out_writer, block_header)
}

pub fn latest_block_summary<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: Option<P2>,
    overwrite: bool,
) -> Result<(), Error> {
    // Validate the output file early so that, in case this fails
    // we don't unnecessarily read the whole database.
    let out_writer: Box<dyn Write> = if let Some(out_path) = output {
        let file = OpenOptions::new()
            .create_new(!overwrite)
            .write(true)
            .open(out_path)?;
        Box::new(file)
    } else {
        Box::new(io::stdout())
    };
    let network_name = match parse_network_name(&db_path) {
        Ok(name) => Some(name),
        Err(io_err) => {
            warn!("Couldn't derive network name from path: {}", io_err);
            None
        }
    };

    let highest_block_header = get_highest_block(db_path)?;
    let block_info = BlockInfo::new(network_name, highest_block_header);
    dump_block_info(&block_info, out_writer)?;

    Ok(())
}
