use std::collections::{BTreeMap, BTreeSet};

use casper_storage::block_store::{
    lmdb::IndexedLmdbBlockStore, BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
};
use casper_types::{
    testing::TestRng, Block, BlockHash, BlockHeaderV2, BlockSignatures, BlockSignaturesV2, BlockV2,
    ChainNameDigest, Digest, EraEndV2, ProtocolVersion, PublicKey, Signature, TestBlockBuilder,
    U512,
};
use once_cell::sync::OnceCell;

use crate::{
    subcommands::purge_signatures::{
        purge::{initialize_indices, purge_signatures_for_blocks, EraWeights},
        Error,
    },
    test_utils::{LmdbTestFixture, KEYS},
};

// Gets a `BlockSignatures` structure from the block
// signatures database.
fn get_sigs_from_db(
    txn: &impl DataReader<BlockHash, BlockSignatures>,
    block_hash: &BlockHash,
) -> BlockSignatures {
    let block_sigs: BlockSignatures = txn.read(*block_hash).unwrap().unwrap();
    assert_eq!(*block_sigs.block_hash(), *block_hash);
    block_sigs
}

#[test]
fn indices_initialization() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock blocks and set an era and height for each one.
    let blocks: Vec<Block> = vec![
        TestBlockBuilder::new()
            .height(100)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(200)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(300)
            .era(20)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(400)
            .era(20)
            .switch_block(false)
            .build(&mut rng)
            .into(),
    ];

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    let switch_blocks: Vec<Block> = vec![
        TestBlockBuilder::new()
            .height(80)
            .era(blocks[0].era_id() - 1)
            .switch_block(true)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(280)
            .era(blocks[2].era_id() - 1)
            .switch_block(true)
            .build(&mut rng)
            .into(),
    ];

    // Insert the blocks into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for block in blocks.iter() {
        let _ = rw_txn.write(block).unwrap();
    }
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();

    let ro_txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&ro_txn, &BTreeSet::from([100, 200, 300])).unwrap();
    ro_txn.commit().unwrap();

    // Make sure we have the relevant blocks in the indices.
    assert_eq!(
        indices.heights.get(&blocks[0].height()).unwrap().0,
        *blocks[0].hash()
    );
    assert_eq!(
        indices.heights.get(&blocks[1].height()).unwrap().0,
        *blocks[1].hash()
    );
    assert_eq!(
        indices.heights.get(&blocks[2].height()).unwrap().0,
        *blocks[2].hash()
    );
    // And that the irrelevant ones are not included.
    assert!(!indices.heights.contains_key(&blocks[3].height()));
    // Make sure we got all the switch blocks.
    assert_eq!(
        *indices.switch_blocks.get(&blocks[0].era_id()).unwrap(),
        *switch_blocks[0].hash()
    );
    assert_eq!(
        *indices.switch_blocks.get(&blocks[2].era_id()).unwrap(),
        *switch_blocks[1].hash()
    );
}

#[test]
fn indices_initialization_with_upgrade() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();
    // Create mock blocks and set an era and height for each one.
    let blocks: Vec<Block> = vec![
        TestBlockBuilder::new()
            .height(80)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(200)
            .era(11)
            .switch_block(false)
            .protocol_version(ProtocolVersion::from_parts(1, 1, 0))
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(290)
            .era(12)
            .switch_block(false)
            .protocol_version(ProtocolVersion::from_parts(2, 0, 0))
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(350)
            .era(13)
            .switch_block(false)
            .protocol_version(ProtocolVersion::from_parts(2, 0, 0))
            .build(&mut rng)
            .into(),
    ];

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    let switch_blocks: Vec<Block> = vec![
        TestBlockBuilder::new()
            .height(60)
            .era(blocks[0].era_id() - 1)
            .switch_block(true)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(180)
            .era(blocks[1].era_id() - 1)
            .switch_block(true)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(250)
            .era(blocks[2].era_id() - 1)
            .switch_block(true)
            .protocol_version(ProtocolVersion::from_parts(1, 1, 0))
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(300)
            .era(blocks[3].era_id() - 1)
            .switch_block(true)
            .protocol_version(ProtocolVersion::from_parts(2, 0, 0))
            .build(&mut rng)
            .into(),
    ];

    // Insert the blocks into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for block in blocks.iter() {
        let _ = rw_txn.write(block).unwrap();
    }
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();

    let ro_txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&ro_txn, &BTreeSet::from([100, 200, 300])).unwrap();
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_blocks[0].height()));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_blocks[1].height()));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_blocks[2].height()));
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_blocks[3].height()));
}

fn new_switch_block_with_weights(
    rng: &mut TestRng,
    era_id: u64,
    height: u64,
    weights: &[(PublicKey, U512)],
    protocol_version: Option<ProtocolVersion>,
) -> Block {
    let switch_block = TestBlockBuilder::new()
        .height(height)
        .era(era_id)
        .switch_block(true)
        .build(rng);

    let next_era_weights: BTreeMap<PublicKey, U512> = weights.iter().cloned().collect();
    let era_end = EraEndV2::new(vec![], vec![], next_era_weights, BTreeMap::new(), 1);

    let switch_block_header = switch_block.header().clone();
    let switch_block_header = BlockHeaderV2::new(
        *switch_block_header.parent_hash(),
        *switch_block_header.state_root_hash(),
        *switch_block_header.body_hash(),
        switch_block_header.random_bit(),
        *switch_block_header.accumulated_seed(),
        Some(era_end),
        switch_block_header.timestamp(),
        switch_block_header.era_id(),
        switch_block_header.height(),
        protocol_version.unwrap_or(switch_block_header.protocol_version()),
        switch_block_header.proposer().clone(),
        switch_block_header.current_gas_price(),
        switch_block_header.last_switch_block_hash(),
        OnceCell::new(),
    );

    Block::from(BlockV2::new_from_header_and_body(
        switch_block_header,
        switch_block.take_body(),
    ))
}

#[test]
fn era_weights() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    let switch_blocks: Vec<Block> = vec![
        new_switch_block_with_weights(&mut rng, 10, 80, &[(KEYS[0].clone(), 100.into())], None),
        new_switch_block_with_weights(&mut rng, 20, 280, &[(KEYS[1].clone(), 100.into())], None),
    ];

    // Insert the blocks into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();
    let ro_txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&ro_txn, &BTreeSet::from([80])).unwrap();
    let mut era_weights = EraWeights::default();

    // Try to update the weights for the first switch block.
    assert!(!era_weights
        .refresh_weights_for_era(&ro_txn, &indices, switch_blocks[0].era_id().successor())
        .unwrap());
    assert_eq!(era_weights.era_id(), switch_blocks[0].era_id().successor());
    assert_eq!(
        *era_weights.weights_mut().get(&KEYS[0]).unwrap(),
        U512::from(100)
    );
    assert!(!era_weights.weights_mut().contains_key(&KEYS[1]));

    // Try to update the weights for the second switch block.
    assert!(!era_weights
        .refresh_weights_for_era(&ro_txn, &indices, switch_blocks[1].era_id().successor())
        .unwrap());
    assert_eq!(era_weights.era_id(), switch_blocks[1].era_id().successor());
    assert_eq!(
        *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
        U512::from(100)
    );
    assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

    // Try to update the weights for the second switch block again.
    assert!(!era_weights
        .refresh_weights_for_era(&ro_txn, &indices, switch_blocks[1].era_id().successor())
        .unwrap());
    assert_eq!(era_weights.era_id(), switch_blocks[1].era_id().successor());
    assert_eq!(
        *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
        U512::from(100)
    );
    assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

    // Try to update the weights for a nonexistent switch block.
    let expected_missing_era_id = switch_blocks[1].era_id().successor().successor();
    match era_weights.refresh_weights_for_era(&ro_txn, &indices, expected_missing_era_id) {
        Err(Error::MissingEraWeights(actual_missing_era_id)) => {
            assert_eq!(expected_missing_era_id, actual_missing_era_id)
        }
        _ => panic!("Unexpected failure"),
    }
    ro_txn.commit().unwrap();
}

#[test]
fn era_weights_with_upgrade() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    let switch_blocks: Vec<Block> = vec![
        // Set an era, height and next era weights for the first one.
        new_switch_block_with_weights(&mut rng, 10, 80, &[(KEYS[0].clone(), 100.into())], None),
        // Set an era, height and next era weights for the second one.
        // Upgrade the version of the second switch block.
        new_switch_block_with_weights(
            &mut rng,
            11,
            280,
            &[(KEYS[1].clone(), 100.into())],
            Some(ProtocolVersion::from_parts(1, 1, 0)),
        ),
    ];

    // Insert the blocks into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();
    let txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&txn, &BTreeSet::from([80, 280])).unwrap();
    let mut era_weights = EraWeights::default();

    assert!(era_weights
        .refresh_weights_for_era(&txn, &indices, switch_blocks[0].era_id().successor())
        .unwrap());

    assert!(!era_weights
        .refresh_weights_for_era(&txn, &indices, switch_blocks[1].era_id().successor())
        .unwrap());

    assert!(era_weights
        .refresh_weights_for_era(&txn, &indices, switch_blocks[0].era_id().successor())
        .unwrap());

    assert!(!era_weights
        .refresh_weights_for_era(&txn, &indices, switch_blocks[1].era_id().successor())
        .unwrap());

    txn.commit().unwrap();
}

#[test]
fn purge_signatures_should_work() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock blocks and set an era and height for each one.
    let blocks: Vec<Block> = vec![
        TestBlockBuilder::new()
            .height(100)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(200)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(300)
            .era(20)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        TestBlockBuilder::new()
            .height(400)
            .era(20)
            .switch_block(false)
            .build(&mut rng)
            .into(),
    ];

    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_digest(Digest::random(&mut rng)),
            )
        })
        .collect();

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    // Add weights for this switch block (500, 500).
    let switch_blocks: Vec<Block> = vec![
        new_switch_block_with_weights(
            &mut rng,
            (blocks[0].era_id() - 1).value(),
            80,
            &[(KEYS[0].clone(), 500.into()), (KEYS[1].clone(), 500.into())],
            None,
        ),
        // Add weights for this switch block (300, 300, 400).
        new_switch_block_with_weights(
            &mut rng,
            (blocks[2].era_id() - 1).value(),
            280,
            &[
                (KEYS[0].clone(), 300.into()),
                (KEYS[1].clone(), 300.into()),
                (KEYS[2].clone(), 400.into()),
            ],
            None,
        ),
    ];

    // Add keys and signatures for block 1.
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);
    // Add keys and signatures for block 2.
    block_signatures[1].insert_signature(KEYS[0].clone(), Signature::System);

    // Add keys and signatures for block 3.
    block_signatures[2].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[2].insert_signature(KEYS[1].clone(), Signature::System);
    block_signatures[2].insert_signature(KEYS[2].clone(), Signature::System);
    // Add keys and signatures for block 4.
    block_signatures[3].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[3].insert_signature(KEYS[2].clone(), Signature::System);

    // Insert the blocks and signatures into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    for (id, block) in blocks.iter().enumerate() {
        let _ = rw_txn.write(block).unwrap();
        let sigs: BlockSignatures = block_signatures[id].clone().into();
        let _ = rw_txn.write(&sigs).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let mut block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();
    let txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&txn, &BTreeSet::from([100, 200, 300, 400])).unwrap();
    txn.commit().unwrap();

    // Purge signatures for blocks 1, 2 and 3 to weak finality.
    let mut txn = block_store.checkout_rw().unwrap();
    assert!(purge_signatures_for_blocks(
        &mut txn,
        &indices,
        BTreeSet::from([100, 200, 300]),
        false
    )
    .is_ok());
    txn.commit().unwrap();

    let txn = block_store.checkout_ro().unwrap();
    let block_1_sigs = get_sigs_from_db(&txn, blocks[0].hash());
    // For block 1, any of the 2 signatures will be fine (500/1000), but
    // not both.
    assert!(
        (block_1_sigs.proofs().contains_key(&KEYS[0])
            && !block_1_sigs.proofs().contains_key(&KEYS[1]))
            || (!block_1_sigs.proofs().contains_key(&KEYS[0])
                && block_1_sigs.proofs().contains_key(&KEYS[1]))
    );

    // Block 2 only had the first signature, which already meets the
    // requirements (500/1000).
    let block_2_sigs = get_sigs_from_db(&txn, blocks[1].hash());
    assert!(block_2_sigs.proofs().contains_key(&KEYS[0]));
    assert!(!block_2_sigs.proofs().contains_key(&KEYS[1]));

    // Block 3 had all the keys (300, 300, 400), so it should have kept
    // the first 2.
    let block_3_sigs = get_sigs_from_db(&txn, blocks[2].hash());
    assert!(block_3_sigs.proofs().contains_key(&KEYS[0]));
    assert!(block_3_sigs.proofs().contains_key(&KEYS[1]));
    assert!(!block_3_sigs.proofs().contains_key(&KEYS[2]));

    // Block 4 had signatures for keys 1 (300) and 3 (400), but it was not
    // included in the purge list, so it should have kept both.
    let block_4_sigs = get_sigs_from_db(&txn, blocks[3].hash());
    assert!(block_4_sigs.proofs().contains_key(&KEYS[0]));
    assert!(!block_4_sigs.proofs().contains_key(&KEYS[1]));
    assert!(block_4_sigs.proofs().contains_key(&KEYS[2]));
    txn.commit().unwrap();

    // Purge signatures for blocks 1 and 4 to no finality.
    let mut txn = block_store.checkout_rw().unwrap();
    assert!(
        purge_signatures_for_blocks(&mut txn, &indices, BTreeSet::from([100, 400]), true).is_ok()
    );
    txn.commit().unwrap();

    let txn = block_store.checkout_ro().unwrap();
    // We should have no record for the signatures of block 1.
    let maybe_block_sigs: Option<BlockSignatures> = txn.read(*blocks[0].hash()).unwrap();
    assert!(maybe_block_sigs.is_none());

    // Block 2 should be the same as before.
    let block_2_sigs = get_sigs_from_db(&txn, blocks[1].hash());
    assert!(block_2_sigs.proofs().contains_key(&KEYS[0]));
    assert!(!block_2_sigs.proofs().contains_key(&KEYS[1]));

    // Block 3 should be the same as before.
    let block_3_sigs = get_sigs_from_db(&txn, blocks[2].hash());
    assert!(block_3_sigs.proofs().contains_key(&KEYS[0]));
    assert!(block_3_sigs.proofs().contains_key(&KEYS[1]));
    assert!(!block_3_sigs.proofs().contains_key(&KEYS[2]));

    // We should have no record for the signatures of block 4.
    let maybe_block_sigs: Option<BlockSignatures> = txn.read(*blocks[3].hash()).unwrap();
    assert!(maybe_block_sigs.is_none());
    txn.commit().unwrap();
}

#[test]
fn purge_signatures_bad_input() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock blocks and set an era and height for each one.
    let blocks: Vec<Block> = vec![
        // Set an era and height for block 1.
        TestBlockBuilder::new()
            .height(100)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        // Set an era and height for block 2.
        TestBlockBuilder::new()
            .height(200)
            .era(20)
            .switch_block(false)
            .build(&mut rng)
            .into(),
    ];

    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_digest(Digest::random(&mut rng)),
            )
        })
        .collect();

    // Create mock switch blocks for each era and set an appropriate era and height for each one.
    let switch_blocks: Vec<Block> = vec![
        // Add weights for this switch block (700, 300).
        new_switch_block_with_weights(
            &mut rng,
            (blocks[0].era_id() - 1).value(),
            80,
            &[(KEYS[0].clone(), 700.into()), (KEYS[1].clone(), 300.into())],
            None,
        ),
        // Add weights for this switch block (400, 600).
        new_switch_block_with_weights(
            &mut rng,
            (blocks[1].era_id() - 1).value(),
            180,
            &[(KEYS[0].clone(), 400.into()), (KEYS[1].clone(), 600.into())],
            None,
        ),
    ];

    // Add keys and signatures for block 1.
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);

    // Add keys and signatures for block 2.
    block_signatures[1].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[1].insert_signature(KEYS[1].clone(), Signature::System);

    // Insert the blocks and signatures into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    for switch_block in switch_blocks.iter() {
        let _ = rw_txn.write(switch_block).unwrap();
    }
    for (id, block) in blocks.iter().enumerate() {
        let _ = rw_txn.write(block).unwrap();
        let sigs: BlockSignatures = block_signatures[id].clone().into();
        let _ = rw_txn.write(&sigs).unwrap();
    }
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let mut block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();
    let txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&txn, &BTreeSet::from([100])).unwrap();
    txn.commit().unwrap();

    // Purge signatures for blocks 1 and 2 to weak finality.
    let mut txn = block_store.checkout_rw().unwrap();
    assert!(
        purge_signatures_for_blocks(&mut txn, &indices, BTreeSet::from([100, 200]), false).is_ok()
    );
    txn.commit().unwrap();

    if let Ok(txn) = block_store.checkout_ro() {
        let block_1_sigs = get_sigs_from_db(&txn, blocks[0].hash());
        // Block 1 has a super-majority signature (700), so the purge would
        // have failed and the signatures are untouched.
        assert!(block_1_sigs.proofs().contains_key(&KEYS[0]));
        assert!(block_1_sigs.proofs().contains_key(&KEYS[1]));

        let block_2_sigs = get_sigs_from_db(&txn, blocks[1].hash());
        // Block 2 wasn't in the purge list, so it should be untouched.
        assert!(block_2_sigs.proofs().contains_key(&KEYS[0]));
        assert!(block_2_sigs.proofs().contains_key(&KEYS[1]));
        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_missing_from_db() {
    let mut rng = TestRng::new();
    let mut fixture = LmdbTestFixture::new();

    // Create mock blocks and set an era and height for each one.
    let blocks: Vec<Block> = vec![
        // Set an era and height for block 1.
        TestBlockBuilder::new()
            .height(100)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
        // Set an era and height for block 2.
        TestBlockBuilder::new()
            .height(200)
            .era(10)
            .switch_block(false)
            .build(&mut rng)
            .into(),
    ];

    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_digest(Digest::random(&mut rng)),
            )
        })
        .collect();

    // Add weights for this switch block (400, 600).
    let switch_block = new_switch_block_with_weights(
        &mut rng,
        (blocks[0].era_id() - 1).value(),
        80,
        &[(KEYS[0].clone(), 400.into()), (KEYS[1].clone(), 600.into())],
        None,
    );

    // Add keys and signatures for block 1 but skip block 2.
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);

    // Insert the blocks and signatures into the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    let _ = rw_txn.write(&switch_block).unwrap();

    for block in blocks.iter() {
        let _ = rw_txn.write(block).unwrap();
    }

    // Only store signatures for block 1.
    let sigs: BlockSignatures = block_signatures[0].clone().into();
    let _ = rw_txn.write(&sigs).unwrap();
    rw_txn.commit().unwrap();

    let (block_store, _store_dir) = fixture.destructure();
    let mut block_store =
        IndexedLmdbBlockStore::new(block_store, None, ProtocolVersion::default()).unwrap();
    let txn = block_store.checkout_ro().unwrap();
    let indices = initialize_indices(&txn, &BTreeSet::from([100, 200])).unwrap();
    txn.commit().unwrap();

    // Purge signatures for blocks 1 and 2 to weak finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    let mut txn = block_store.checkout_rw().unwrap();
    assert!(
        purge_signatures_for_blocks(&mut txn, &indices, BTreeSet::from([100, 200]), false).is_ok()
    );
    txn.commit().unwrap();

    if let Ok(txn) = block_store.checkout_ro() {
        let block_1_sigs = get_sigs_from_db(&txn, blocks[0].hash());
        // Block 1 had both keys (400, 600), so it should have kept
        // the first one.
        assert!(block_1_sigs.proofs().contains_key(&KEYS[0]));
        assert!(!block_1_sigs.proofs().contains_key(&KEYS[1]));

        // We should have no record for the signatures of block 2.
        let maybe_block_sigs: Option<BlockSignatures> = txn.read(*blocks[1].hash()).unwrap();
        assert!(maybe_block_sigs.is_none());

        txn.commit().unwrap();
    };

    // Purge signatures for blocks 1 and 2 to no finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    let mut txn = block_store.checkout_rw().unwrap();
    assert!(
        purge_signatures_for_blocks(&mut txn, &indices, BTreeSet::from([100, 200]), true).is_ok()
    );
    txn.commit().unwrap();

    if let Ok(txn) = block_store.checkout_ro() {
        // We should have no record for the signatures of block 1.
        let maybe_block_sigs: Option<BlockSignatures> = txn.read(*blocks[0].hash()).unwrap();
        assert!(maybe_block_sigs.is_none());

        // We should have no record for the signatures of block 2.
        let maybe_block_sigs: Option<BlockSignatures> = txn.read(*blocks[1].hash()).unwrap();
        assert!(maybe_block_sigs.is_none());
        txn.commit().unwrap();
    };
}
