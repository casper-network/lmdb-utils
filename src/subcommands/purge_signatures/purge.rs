use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    path::Path,
};

use casper_hashing::Digest;
use casper_node::types::{BlockHash, BlockHeader};
use casper_types::{EraId, ProtocolVersion, PublicKey, U512};
use lmdb::{Cursor, Database, Environment, Error as LmdbError, Transaction, WriteFlags};
use log::{error, info, warn};

use crate::common::{
    db::{self, BlockHeaderDatabase, BlockMetadataDatabase, Database as _, STORAGE_FILE_NAME},
    lmdb_utils,
    progress::ProgressTracker,
};

use super::{block_signatures::BlockSignatures, signatures::strip_signatures, Error};

/// Structure to hold lookup information for a set of block headers.
#[derive(Default)]
pub(crate) struct Indices {
    /// Hold the hash and the header of a block keyed by its height.
    pub(crate) heights: BTreeMap<u64, (BlockHash, BlockHeader)>,
    /// Hold the hash of switch blocks keyed by the era for which they hold
    /// the weights.
    pub(crate) switch_blocks: BTreeMap<EraId, BlockHash>,
    /// Hold the heights of switch blocks before upgrades.
    pub(crate) switch_blocks_before_upgrade: BTreeSet<u64>,
}

/// Cache-like structure to store the validator weights for an era.
#[derive(Default)]
pub(crate) struct EraWeights {
    era_id: EraId,
    weights: BTreeMap<PublicKey, U512>,
    era_after_upgrade: bool,
}

impl EraWeights {
    /// Update the internal structure to hold the validator weights for
    /// the era given as input.
    ///
    /// Return value is `true` when the switch block used to populate the
    /// weights is a switch block right before an upgrade or `false` otherwise.
    pub(crate) fn refresh_weights_for_era<T: Transaction>(
        &mut self,
        txn: &T,
        db: Database,
        indices: &Indices,
        era_id: EraId,
    ) -> Result<bool, Error> {
        // If we already have the requested era, exit early.
        if self.era_id == era_id {
            return Ok(self.era_after_upgrade);
        }
        // Get the required era's associated switch block.
        let switch_block_hash = indices
            .switch_blocks
            .get(&era_id)
            .ok_or(Error::MissingEraWeights(era_id))?;
        // Deserialize it.
        let switch_block_header: BlockHeader =
            bincode::deserialize(txn.get(db, &switch_block_hash)?)
                .map_err(|bincode_err| Error::HeaderParsing(*switch_block_hash, bincode_err))?;
        // Check if this switch block is the last in the era before an upgrade.
        self.era_after_upgrade = indices
            .switch_blocks_before_upgrade
            .contains(&switch_block_header.height());
        // Get the weights.
        let weights = switch_block_header
            .next_era_validator_weights()
            .cloned()
            .ok_or(Error::MissingEraWeights(era_id))?;
        self.weights = weights;
        self.era_id = era_id;
        Ok(self.era_after_upgrade)
    }

    #[cfg(test)]
    pub(crate) fn era_id(&self) -> EraId {
        self.era_id
    }

    #[cfg(test)]
    pub(crate) fn weights_mut(&mut self) -> &mut BTreeMap<PublicKey, U512> {
        &mut self.weights
    }
}

/// Creates a collection of indices to store lookup information for a given
/// list of block heights.
pub(crate) fn initialize_indices(
    env: &Environment,
    needed_heights: &BTreeSet<u64>,
) -> Result<Indices, Error> {
    let mut indices = Indices::default();
    let txn = env.begin_ro_txn()?;
    let header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };

    let mut maybe_progress_tracker = match lmdb_utils::entry_count(&txn, header_db).ok() {
        Some(entry_count) => Some(
            ProgressTracker::new(
                entry_count,
                Box::new(|completion| info!("Header database parsing {}% complete...", completion)),
            )
            .map_err(|_| Error::EmptyDatabase)?,
        ),
        None => {
            info!("Skipping progress tracking for header database parsing");
            None
        }
    };

    {
        let mut last_blocks_before_upgrade: BTreeMap<ProtocolVersion, u64> = BTreeMap::default();
        // Iterate through all block headers.
        let mut cursor = txn.open_ro_cursor(header_db)?;
        for (raw_key, raw_value) in cursor.iter() {
            if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
                progress_tracker.advance_by(1);
            }
            // Deserialize the block hash.
            let block_hash: BlockHash = match Digest::try_from(raw_key) {
                Ok(digest) => digest.into(),
                Err(digest_parsing_err) => {
                    error!("Skipping block header because of invalid hash {raw_key:?}: {digest_parsing_err}");
                    continue;
                }
            };
            // Deserialize the header.
            let block_header: BlockHeader = bincode::deserialize(raw_value)
                .map_err(|bincode_err| Error::HeaderParsing(block_hash, bincode_err))?;
            let block_height = block_header.height();
            // We store all switch block hashes keyed by the era for which they
            // hold the weights.
            if block_header.is_switch_block() {
                let _ = indices
                    .switch_blocks
                    .insert(block_header.era_id().successor(), block_hash);
                // Store the highest switch block height for each protocol
                // version we encounter.
                match last_blocks_before_upgrade.entry(block_header.protocol_version()) {
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(block_height);
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        if *occupied_entry.get() < block_height {
                            occupied_entry.insert(block_height);
                        }
                    }
                }
            }
            // If this block is on our list, store its hash and header in the
            // indices. We store the header to avoid looking it up again in the
            // future since we know we will need it and we expect
            // `needed_heights` to be a relatively small list.
            if needed_heights.contains(&block_height)
                && indices
                    .heights
                    .insert(block_height, (block_hash, block_header))
                    .is_some()
            {
                return Err(Error::DuplicateBlock(block_height));
            };
        }
        // Remove the entry for the highest known protocol version as it hasn't
        // had an upgrade yet.
        let _ = last_blocks_before_upgrade.pop_last();
        // Store the heights of the relevant switch blocks in the indices.
        indices
            .switch_blocks_before_upgrade
            .extend(last_blocks_before_upgrade.into_values());
    }
    txn.commit()?;
    Ok(indices)
}

/// Purges finality signatures from a database for all blocks of heights found
/// in `heights_to_visit`.
///
/// If the `full_purge` flag is set, all the signatures for the associated
/// block will be purged by deleting the record in the block signatures
/// database.
///
/// If the `full_purge` flag is not set, signatures will be purged until the
/// remaining set of signatures gives the block weak but not strict finality.
/// If this is not possible for that block given its signature set and the era
/// weights, it is skipped and a message is logged.
pub(crate) fn purge_signatures_for_blocks(
    env: &Environment,
    indices: &Indices,
    heights_to_visit: BTreeSet<u64>,
    full_purge: bool,
) -> Result<(), Error> {
    let mut txn = env.begin_rw_txn()?;
    let header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };
    let signatures_db = unsafe { txn.open_db(Some(BlockMetadataDatabase::db_name()))? };

    let mut era_weights = EraWeights::default();

    let mut progress_tracker = ProgressTracker::new(
        heights_to_visit.len(),
        Box::new(if full_purge {
            |completion| {
                info!(
                    "Signature purging to no finality {}% complete...",
                    completion
                )
            }
        } else {
            |completion| {
                info!(
                    "Signature purging to weak finality {}% complete...",
                    completion
                )
            }
        }),
    )
    .map_err(|_| Error::EmptyBlockList)?;

    for height in heights_to_visit {
        // Get the block hash and header from the indices for this height.
        let (block_hash, block_header) = match indices.heights.get(&height) {
            Some((block_hash, block_header)) => {
                // We don't strip signatures for the genesis block.
                if block_header.era_id().is_genesis() {
                    warn!("Cannot strip signatures for genesis block");
                    progress_tracker.advance_by(1);
                    continue;
                }
                (block_hash, block_header)
            }
            None => {
                // Skip blocks which are not in the database.
                warn!("Block at height {height} is not present in the database");
                progress_tracker.advance_by(1);
                continue;
            }
        };
        let block_height = block_header.height();
        let era_id = block_header.era_id();
        // Make sure we have the correct era weights for this block before
        // trying to strip any signatures.
        let era_after_upgrade =
            era_weights.refresh_weights_for_era(&txn, header_db, indices, era_id)?;

        let mut block_signatures: BlockSignatures = match txn.get(signatures_db, &block_hash) {
            Ok(raw_signatures) => bincode::deserialize(raw_signatures)
                .map_err(|bincode_err| Error::SignaturesParsing(*block_hash, bincode_err))?,
            Err(LmdbError::NotFound) => {
                // Skip blocks which have no signature entry in the database.
                warn!(
                    "No signature entry in the database for block \
                    {block_hash} at height {block_height}"
                );
                progress_tracker.advance_by(1);
                continue;
            }
            Err(lmdb_err) => return Err(Error::Database(lmdb_err)),
        };

        if full_purge {
            // Delete the record completely from the database.
            txn.del(signatures_db, &block_hash, None)?;
        } else if strip_signatures(&mut block_signatures, &era_weights.weights) {
            if era_after_upgrade {
                warn!(
                    "Using possibly inaccurate weights to purge signatures \
                    for block {block_hash} at height {block_height}"
                );
            }
            // Serialize the remaining signatures and overwrite the database
            // entry.
            let serialized_signatures = bincode::serialize(&block_signatures)
                .map_err(|bincode_err| Error::Serialize(*block_hash, bincode_err))?;
            txn.put(
                signatures_db,
                &block_hash,
                &serialized_signatures,
                WriteFlags::default(),
            )?;
        } else {
            warn!("Couldn't strip signatures for block {block_hash} at height {block_height}");
        }
        progress_tracker.advance_by(1);
    }
    txn.commit()?;
    Ok(())
}

pub fn purge_signatures<P: AsRef<Path>>(
    db_path: P,
    weak_finality_block_list: BTreeSet<u64>,
    no_finality_block_list: BTreeSet<u64>,
) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = db::db_env(storage_path)?;
    let heights_to_visit = weak_finality_block_list
        .union(&no_finality_block_list)
        .copied()
        .collect();
    let indices = initialize_indices(&env, &heights_to_visit)?;
    if !weak_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&env, &indices, weak_finality_block_list, false)?;
    }
    if !no_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&env, &indices, no_finality_block_list, true)?;
    }
    Ok(())
}
