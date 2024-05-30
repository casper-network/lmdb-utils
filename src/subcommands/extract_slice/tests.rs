use casper_storage::{
    block_store::{
        types::{BlockExecutionResults, BlockHashHeightAndEra, BlockTransfers},
        BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
    },
    global_state::{store::StoreExt, transaction_source::TransactionSource, trie::Trie},
};
use casper_types::bytesrepr::{Bytes, ToBytes};
use casper_types::{
    execution::ExecutionResult, testing::TestRng, Block, BlockHeader, Digest, TestBlockBuilder,
    Transaction, Transfer,
};
use lmdb::Transaction as LmdbTransaction;

use crate::{
    subcommands::{
        extract_slice::{global_state, storage},
        trie_compact::{create_data_access_layer, tests::create_data, DEFAULT_MAX_DB_SIZE},
    },
    test_utils::LmdbTestFixture,
};

#[test]
fn transfer_blocks() {
    const TRANSFER_COUNT: usize = 3;

    const TRANSACTION_COUNT: usize = 7;
    let mut rng = TestRng::new();

    let mut source_fixture = LmdbTestFixture::new();

    let transactions: Vec<Transaction> = (0..TRANSACTION_COUNT as u8)
        .map(|_| Transaction::random(&mut rng))
        .collect();

    let mut blocks: Vec<Block> = vec![];
    let mut block_transactions_map: Vec<Vec<usize>> = vec![];
    blocks.push(
        TestBlockBuilder::new()
            .transactions([0, 1, 2].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![0, 1, 2]);
    blocks.push(
        TestBlockBuilder::new()
            .transactions([3, 4].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![3, 4]);
    blocks.push(
        TestBlockBuilder::new()
            .transactions([5, 6].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![5, 6]);

    let exec_results: Vec<ExecutionResult> = (0..TRANSACTION_COUNT as u8)
        .map(|_| ExecutionResult::random(&mut rng))
        .collect();

    let mut rw_txn = source_fixture.block_store.checkout_rw().unwrap();

    // Insert the transactions in the database.
    for transaction in transactions.iter() {
        let _ = rw_txn.write(transaction).unwrap();
    }

    // Insert the 3 blocks into the database.
    for (block_id, block) in blocks.iter().enumerate() {
        let block_hash = rw_txn.write(block).unwrap();

        let height = block.height();
        let era = block.era_id();

        let block_info = BlockHashHeightAndEra::new(block_hash, height, era);

        let block_exec_results = BlockExecutionResults {
            block_info,
            exec_results: block_transactions_map[block_id]
                .iter()
                .map(|id| (transactions[*id].hash(), exec_results[*id].clone()))
                .collect(),
        };
        rw_txn.write(&block_exec_results).unwrap();
    }
    rw_txn.commit().unwrap();

    let destination_fixture = LmdbTestFixture::new();

    let block_hash_0 = *blocks[0].hash();
    let expected_state_root_hash = *blocks[0].state_root_hash();
    let actual_state_root_hash = storage::transfer_block_info(
        source_fixture.tmp_dir.path(),
        destination_fixture.tmp_dir.path(),
        block_hash_0,
    )
    .unwrap();
    assert_eq!(expected_state_root_hash, actual_state_root_hash);

    {
        let txn = destination_fixture.block_store.checkout_ro().unwrap();
        let actual_block_header: Option<BlockHeader> = txn.read(block_hash_0).unwrap();
        assert_eq!(actual_block_header, Some(blocks[0].clone_header()));

        let actual_block: Option<Block> = txn.read(block_hash_0).unwrap();
        assert_eq!(actual_block, Some(blocks[0].clone()));

        for transaction_hash in actual_block.unwrap().all_transaction_hashes() {
            // Check if the transactions are stored
            let transaction: Option<Transaction> = txn.read(transaction_hash).unwrap();
            assert!(transaction.is_some());
            assert_eq!(transaction_hash, transaction.unwrap().hash());

            let exec_result: Option<ExecutionResult> = txn.read(transaction_hash).unwrap();
            assert!(exec_result.is_some());
        }

        let block_header: Option<BlockHeader> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block_header.is_none());
        let block_header: Option<BlockHeader> = txn.read(*blocks[2].hash()).unwrap();
        assert!(block_header.is_none());
        txn.commit().unwrap();
    }

    let block_hash_1 = *blocks[1].hash();
    let transfers: Vec<Transfer> = (0..TRANSFER_COUNT as u8)
        .map(|_| Transfer::random(&mut rng))
        .collect();

    // Put some mock data in the transfer DB under block hash 1.
    {
        let mut txn = source_fixture.block_store.checkout_rw().unwrap();

        let block_transfers = BlockTransfers {
            block_hash: block_hash_1,
            transfers: transfers.clone(),
        };

        let _ = txn.write(&block_transfers).unwrap();
        txn.commit().unwrap();
    }

    let expected_state_root_hash = *blocks[1].state_root_hash();
    let actual_state_root_hash = storage::transfer_block_info(
        source_fixture.tmp_dir.path(),
        destination_fixture.tmp_dir.path(),
        block_hash_1,
    )
    .unwrap();
    assert_eq!(expected_state_root_hash, actual_state_root_hash);

    {
        let txn = destination_fixture.block_store.checkout_ro().unwrap();
        let actual_block_header: Option<BlockHeader> = txn.read(block_hash_1).unwrap();
        assert_eq!(actual_block_header, Some(blocks[1].clone_header()));

        let actual_block: Option<Block> = txn.read(block_hash_1).unwrap();
        assert_eq!(actual_block, Some(blocks[1].clone()));

        let actual_transfers: Option<Vec<Transfer>> = txn.read(block_hash_1).unwrap();
        assert_eq!(Some(transfers), actual_transfers);

        for transaction_hash in actual_block.unwrap().all_transaction_hashes() {
            // Check if the transactions are stored
            let transaction: Option<Transaction> = txn.read(transaction_hash).unwrap();
            assert!(transaction.is_some());
            assert_eq!(transaction_hash, transaction.unwrap().hash());

            let exec_result: Option<ExecutionResult> = txn.read(transaction_hash).unwrap();
            assert!(exec_result.is_some());
        }

        let block_header: Option<BlockHeader> = txn.read(*blocks[2].hash()).unwrap();
        assert!(block_header.is_none());

        txn.commit().unwrap();
    }
}

#[test]
fn transfer_global_state_information() {
    let source_tmp_dir = tempfile::tempdir().unwrap();
    let destination_tmp_dir = tempfile::tempdir().unwrap();
    let max_db_size = DEFAULT_MAX_DB_SIZE
        .parse()
        .expect("should be able to parse max db size");

    // Construct mock data.
    let data = create_data();

    let source = create_data_access_layer(source_tmp_dir.path(), max_db_size, true).unwrap();

    let source_store = source.state().trie_store();
    {
        // Put the generated data into the source trie.
        let mut txn = source
            .state()
            .environment()
            .create_read_write_txn()
            .unwrap();
        let items = data.iter().map(Into::into);
        source_store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    let destination =
        create_data_access_layer(destination_tmp_dir.path(), max_db_size, true).unwrap();

    // Copy from `node2`, the root of the created trie. All data under node 2,
    // which has leaf 2 and 3 under it, should be copied.
    global_state::transfer_global_state(
        source_tmp_dir.path(),
        destination_tmp_dir.path(),
        data[4].0,
    )
    .unwrap();

    let destination_store = destination.state().trie_store(); // LmdbTrieStore::new(&dst_env, None, DatabaseFlags::empty()).unwrap();
    {
        let txn = destination
            .state()
            .environment()
            .create_read_write_txn()
            .unwrap();
        let keys = [data[1].0, data[2].0, data[4].0];
        let entries: Vec<Option<Trie<Bytes, Bytes>>> =
            destination_store.get_many(&txn, keys.iter()).unwrap();
        for entry in entries {
            match entry {
                Some(trie) => {
                    let trie_in_data = data.iter().find(|test_data| test_data.1 == trie);
                    // Check we are not missing anything since all data under
                    // node 2 should be copied.
                    assert!(trie_in_data.is_some());
                    // Hashes should be equal.
                    assert_eq!(
                        trie_in_data.unwrap().0,
                        Digest::hash(&trie.to_bytes().unwrap())
                    );
                }
                None => panic!(),
            }
        }
        txn.commit().unwrap();
    }

    source_tmp_dir.close().unwrap();
    destination_tmp_dir.close().unwrap();
}
