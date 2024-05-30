use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    path::Path,
};

use casper_storage::block_store::{
    lmdb::{IndexedLmdbBlockStore, LmdbBlockStore},
    types::{BlockHeight, Tip},
    BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
};
use casper_types::{BlockHash, BlockHeader, BlockSignatures};
use casper_types::{EraId, ProtocolVersion, PublicKey, U512};
use log::{info, warn};

use crate::common::{
    db::{
        DEFAULT_MAX_BLOCK_STORE_SIZE, DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
        DEFAULT_MAX_DEPLOY_STORE_SIZE,
    },
    progress::ProgressTracker,
};

use super::{signatures::strip_signatures, Error};

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
    pub(crate) fn refresh_weights_for_era(
        &mut self,
        txn: &impl DataReader<BlockHash, BlockHeader>,
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
            .ok_or_else(|| Error::MissingEraWeights(era_id))?;
        // Deserialize it.
        let maybe_switch_block_header: Option<BlockHeader> = txn.read(*switch_block_hash)?;
        let switch_block_header = if let Some(header) = maybe_switch_block_header {
            header
        } else {
            return Err(Error::MissingBlockHeader(*switch_block_hash));
        };
        // Check if this switch block is the last in the era before an upgrade.
        self.era_after_upgrade = indices
            .switch_blocks_before_upgrade
            .contains(&switch_block_header.height());
        // Get the weights.
        let weights = switch_block_header
            .next_era_validator_weights()
            .cloned()
            .ok_or_else(|| Error::MissingEraWeights(era_id))?;
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
    ro_txn: &(impl DataReader<Tip, BlockHeader> + DataReader<BlockHeight, BlockHeader>),
    needed_heights: &BTreeSet<u64>,
) -> Result<Indices, Error> {
    let mut indices = Indices::default();

    let mut block_heights = vec![];
    let latest_block_header =
        DataReader::<Tip, BlockHeader>::read(ro_txn, Tip)?.ok_or(Error::EmptyDatabase)?;
    let maybe_block_heights = 0..=latest_block_header.height();
    for height in maybe_block_heights {
        if DataReader::<BlockHeight, BlockHeader>::exists(ro_txn, height)? {
            block_heights.push(height);
        }
    }

    let mut progress_tracker = ProgressTracker::new(
        block_heights.len(),
        Box::new(|completion| info!("Header database parsing {}% complete...", completion)),
    )
    .map_err(|_| Error::EmptyDatabase)?;

    {
        let mut last_blocks_before_upgrade: BTreeMap<ProtocolVersion, u64> = BTreeMap::default();

        for block_height in block_heights {
            if let Some(block_header) =
                DataReader::<BlockHeight, BlockHeader>::read(ro_txn, block_height)?
            {
                let block_height = block_header.height();
                let block_hash = block_header.block_hash();
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

            progress_tracker.advance_by(1);
        }

        // Remove the entry for the highest known protocol version as it hasn't
        // had an upgrade yet.
        let _ = last_blocks_before_upgrade.pop_last();
        // Store the heights of the relevant switch blocks in the indices.
        indices
            .switch_blocks_before_upgrade
            .extend(last_blocks_before_upgrade.into_values());
    }
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
    rw_txn: &mut (impl DataReader<BlockHash, BlockSignatures>
              + DataReader<BlockHash, BlockHeader>
              + DataWriter<BlockHash, BlockSignatures>),
    indices: &Indices,
    heights_to_visit: BTreeSet<u64>,
    full_purge: bool,
) -> Result<(), Error> {
    let mut era_weights = EraWeights::default();
    let mut progress_tracker: ProgressTracker = ProgressTracker::new(
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
        let era_after_upgrade = era_weights.refresh_weights_for_era(rw_txn, indices, era_id)?;

        let mut block_signatures: BlockSignatures = match rw_txn.read(*block_hash)? {
            Some(signatures) => signatures,
            None => {
                // Skip blocks which have no signature entry in the database.
                warn!(
                    "No signature entry in the database for block \
                    {block_hash} at height {block_height}"
                );
                progress_tracker.advance_by(1);
                continue;
            }
        };

        if full_purge {
            // Delete the record completely from the database.
            rw_txn.delete(*block_hash)?;
        } else if strip_signatures(&mut block_signatures, &era_weights.weights) {
            if era_after_upgrade {
                warn!(
                    "Using possibly inaccurate weights to purge signatures \
                    for block {block_hash} at height {block_height}"
                );
            }

            // Overwrite the database with the remaining signatures entry.
            rw_txn.write(&block_signatures)?;
        } else {
            warn!("Couldn't strip signatures for block {block_hash} at height {block_height}");
        }
        progress_tracker.advance_by(1);
    }
    Ok(())
}

pub fn purge_signatures<P: AsRef<Path>>(
    db_path: P,
    weak_finality_block_list: BTreeSet<u64>,
    no_finality_block_list: BTreeSet<u64>,
) -> Result<(), Error> {
    let heights_to_visit = weak_finality_block_list
        .union(&no_finality_block_list)
        .copied()
        .collect();

    let block_store = LmdbBlockStore::new(
        db_path.as_ref(),
        DEFAULT_MAX_BLOCK_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_STORE_SIZE
            + DEFAULT_MAX_DEPLOY_METADATA_STORE_SIZE,
    )?;
    let mut indexed_block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::from_parts(0, 0, 0))?;

    let ro_txn = indexed_block_store.checkout_ro()?;
    let indices = initialize_indices(&ro_txn, &heights_to_visit)?;
    ro_txn.commit()?;

    let mut rw_txn = indexed_block_store.checkout_rw()?;
    if !weak_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&mut rw_txn, &indices, weak_finality_block_list, false)?;
    }
    if !no_finality_block_list.is_empty() {
        purge_signatures_for_blocks(&mut rw_txn, &indices, no_finality_block_list, true)?;
    }
    rw_txn.commit()?;
    Ok(())
}
