use std::time::{Duration, Instant};

use casper_storage::data_access_layer::DataAccessLayer;
use casper_storage::global_state::state::lmdb::LmdbGlobalState;
use casper_storage::global_state::transaction_source::{Readable, TransactionSource, Writable};
use casper_storage::global_state::trie::Trie;
use casper_storage::global_state::trie_store::lmdb::LmdbTrieStore;
use lmdb::{RwTransaction, Transaction};
use log::{info, warn};

use casper_types::{
    bytesrepr::{self, Bytes, ToBytes},
    Key, StoredValue,
};
use casper_types::{Digest, Pointer};

fn memoized_find_missing_descendants(
    value_bytes: Bytes,
    trie_store: &LmdbTrieStore,
    txn: &RwTransaction<'_>,
    missing_trie_keys: &mut Vec<Digest>,
    time_in_missing_trie_keys: &mut Duration,
) -> Result<(), anyhow::Error> {
    // A first bytes of `0` indicates a leaf. We short-circuit the function here to speed things up.
    if let Some(0u8) = value_bytes.first() {
        return Ok(());
    }
    let start_trie_keys = Instant::now();
    let trie: Trie<Key, StoredValue> = bytesrepr::deserialize(value_bytes.into())
        .map_err(|err| anyhow::anyhow!("couldn't deserialize trie: {:?}", err))?;
    match trie {
        Trie::Leaf { .. } => {
            // If `bytesrepr` is functioning correctly, this should never be reached (see
            // optimization above), but it is still correct do nothing here.
            warn!("did not expect to see a trie leaf in `find_missing_descendents` after shortcut");
        }
        Trie::Node { pointer_block } => {
            for (_index, ptr) in pointer_block.as_indexed_pointers() {
                find_missing_trie_keys(ptr, missing_trie_keys, trie_store, txn)?;
            }
        }
        Trie::Extension { affix: _, pointer } => {
            find_missing_trie_keys(pointer, missing_trie_keys, trie_store, txn)?;
        }
    }
    *time_in_missing_trie_keys += start_trie_keys.elapsed();
    Ok(())
}

fn find_missing_trie_keys(
    ptr: Pointer,
    missing_trie_keys: &mut Vec<Digest>,
    handle: &LmdbTrieStore,
    txn: &RwTransaction<'_>,
) -> Result<(), anyhow::Error> {
    let ptr = match ptr {
        Pointer::LeafPointer(pointer) | Pointer::NodePointer(pointer) => pointer,
    };
    let existing = txn.read(
        handle.get_db(),
        &ptr.to_bytes()
            .map_err(|err| anyhow::anyhow!("couldn't serialize trie pointer: {:?}", err))?,
    )?;
    if existing.is_none() {
        missing_trie_keys.push(ptr);
    }
    Ok(())
}

pub fn copy_state_root(
    state_root: Digest,
    source: &DataAccessLayer<LmdbGlobalState>,
    destination: &DataAccessLayer<LmdbGlobalState>,
) -> Result<(), anyhow::Error> {
    let mut missing_trie_keys = vec![state_root];
    let start_time = Instant::now();
    let mut heartbeat_interval = Instant::now();

    let mut total_tries: u64 = 0;
    let mut total_bytes: u64 = 0;

    let mut time_searching_for_trie_keys = Duration::from_secs(0);

    while let Some(next_trie_key) = missing_trie_keys.pop() {
        // For user feedback, update on progress if this takes longer than 10 seconds.
        if heartbeat_interval.elapsed().as_secs() > 10 {
            info!(
                "trie migration progress: bytes copied {}, tries copied {}",
                total_bytes, total_tries,
            );
            heartbeat_interval = Instant::now();
        }

        let source_store = source.state().trie_store();
        let destination_store = destination.state().trie_store();
        let trie_key_bytes = next_trie_key
            .to_bytes()
            .map_err(|err| anyhow::anyhow!("couldn't serialize trie key: {:?}", err))?;

        let read_txn = source.state().environment().create_read_txn()?;
        let mut write_txn = destination.state().environment().create_read_write_txn()?;

        match read_txn.read(source_store.get_db(), &trie_key_bytes)? {
            Some(value_bytes) => {
                let key_bytes = next_trie_key
                    .to_bytes()
                    .map_err(|err| anyhow::anyhow!("couldn't serialize trie key: {:?}", err))?;
                let read_bytes = key_bytes.len() as u64 + value_bytes.len() as u64;
                total_bytes += read_bytes;
                total_tries += 1;

                write_txn.write(destination_store.get_db(), &key_bytes, &value_bytes)?;

                memoized_find_missing_descendants(
                    value_bytes,
                    destination_store,
                    &write_txn,
                    &mut missing_trie_keys,
                    &mut time_searching_for_trie_keys,
                )?;
            }
            None => {
                return Err(anyhow::anyhow!(
                    "error migrating state root {} {}, ",
                    state_root,
                    next_trie_key
                ));
            }
        }
        read_txn.commit()?;
        write_txn.commit()?;
    }

    info!(
        "Trie migration complete\nTotal bytes: {}\n\
            Total tries: {}\nMigration duration (us): {}\n\
            Trie key search duration (us):{}",
        total_bytes,
        total_tries,
        start_time.elapsed().as_micros(),
        time_searching_for_trie_keys.as_micros(),
    );
    Ok(())
}
