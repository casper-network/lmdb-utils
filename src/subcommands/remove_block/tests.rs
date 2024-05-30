use casper_storage::block_store::{
    types::{BlockExecutionResults, BlockHashHeightAndEra},
    BlockStoreProvider, BlockStoreTransaction, DataReader, DataWriter,
};
use casper_types::{
    execution::ExecutionResult, testing::TestRng, Block, BlockHeader, TestBlockBuilder, Transaction,
};

use crate::{
    subcommands::remove_block::{remove::remove_block, Error},
    test_utils::LmdbTestFixture,
};

#[test]
fn remove_block_should_work() {
    const TRANSACTION_COUNT: usize = 4;

    let mut rng = TestRng::new();
    let mut test_fixture = LmdbTestFixture::new();

    let transactions: Vec<Transaction> = (0..TRANSACTION_COUNT as u8)
        .map(|_| Transaction::random(&mut rng))
        .collect();
    let mut blocks: Vec<Block> = vec![];
    let mut block_transactions_map: Vec<Vec<usize>> = vec![];
    blocks.push(
        TestBlockBuilder::new()
            .transactions([0, 1].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![0, 1]);
    blocks.push(
        TestBlockBuilder::new()
            .transactions([2, 3].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![2, 3]);

    let exec_results: Vec<ExecutionResult> = (0..TRANSACTION_COUNT as u8)
        .map(|_| ExecutionResult::random(&mut rng))
        .collect();

    let mut rw_txn = test_fixture.block_store.checkout_rw().unwrap();

    for transaction in transactions.iter() {
        let _ = rw_txn.write(transaction).unwrap();
    }

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

    assert!(remove_block(test_fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = test_fixture.block_store.checkout_ro().unwrap();
        let removed_block_header: Option<BlockHeader> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block_header.is_none());
        let removed_block: Option<Block> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block.is_none());

        let block_header: Option<BlockHeader> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block_header.is_some());
        let block: Option<Block> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block.is_some());

        // Transactions included in the removed blocks should be removed.
        let transaction: Option<Transaction> = txn.read(transactions[0].hash()).unwrap();
        assert!(transaction.is_none());
        let transaction: Option<Transaction> = txn.read(transactions[1].hash()).unwrap();
        assert!(transaction.is_none());

        // Exec results for transactions included in the removed block should be removed.
        let exec_result: Option<ExecutionResult> = txn.read(transactions[0].hash()).unwrap();
        assert!(exec_result.is_none());
        let exec_result: Option<ExecutionResult> = txn.read(transactions[1].hash()).unwrap();
        assert!(exec_result.is_none());
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_no_deploys() {
    const TRANSACTION_COUNT: usize = 3;

    let mut rng = TestRng::new();
    let mut test_fixture = LmdbTestFixture::new();

    let transactions: Vec<Transaction> = (0..TRANSACTION_COUNT as u8)
        .map(|_| Transaction::random(&mut rng))
        .collect();
    let mut blocks: Vec<Block> = vec![];
    let mut block_transactions_map: Vec<Vec<usize>> = vec![];

    blocks.push(TestBlockBuilder::new().build(&mut rng).into());
    block_transactions_map.push(vec![]);
    blocks.push(
        TestBlockBuilder::new()
            .transactions([1, 2].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![1, 2]);

    let exec_results: Vec<ExecutionResult> = (0..TRANSACTION_COUNT as u8)
        .map(|_| ExecutionResult::random(&mut rng))
        .collect();

    let mut rw_txn = test_fixture.block_store.checkout_rw().unwrap();

    for i in 1..TRANSACTION_COUNT {
        let _ = rw_txn.write(&transactions[i]).unwrap();
    }

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

    assert!(remove_block(test_fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = test_fixture.block_store.checkout_ro().unwrap();
        let removed_block_header: Option<BlockHeader> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block_header.is_none());
        let removed_block: Option<Block> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block.is_none());

        let block_header: Option<BlockHeader> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block_header.is_some());
        let block: Option<Block> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block.is_some());

        let transaction: Option<Transaction> = txn.read(transactions[0].hash()).unwrap();
        assert!(transaction.is_none());

        let transaction: Option<Transaction> = txn.read(transactions[1].hash()).unwrap();
        assert!(transaction.is_some());
        let transaction: Option<Transaction> = txn.read(transactions[2].hash()).unwrap();
        assert!(transaction.is_some());

        let exec_result: Option<ExecutionResult> = txn.read(transactions[0].hash()).unwrap();
        assert!(exec_result.is_none());
        let exec_result: Option<ExecutionResult> = txn.read(transactions[1].hash()).unwrap();
        assert!(exec_result.is_some());
        let exec_result: Option<ExecutionResult> = txn.read(transactions[2].hash()).unwrap();
        assert!(exec_result.is_some());

        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_missing_header() {
    let test_fixture = LmdbTestFixture::new();

    let mut rng = TestRng::new();
    let block_hash = *Block::from(TestBlockBuilder::new().build(&mut rng)).hash();
    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::MissingHeader(actual_block_hash) if block_hash == actual_block_hash)
    );
}

#[test]
fn remove_block_missing_body() {
    const TRANSACTION_COUNT: usize = 4;

    let mut rng = TestRng::new();
    let mut test_fixture = LmdbTestFixture::new();

    let transactions: Vec<Transaction> = (0..TRANSACTION_COUNT as u8)
        .map(|_| Transaction::random(&mut rng))
        .collect();
    let mut blocks: Vec<Block> = vec![];
    let mut block_transactions_map: Vec<Vec<usize>> = vec![];
    blocks.push(
        TestBlockBuilder::new()
            .transactions([0, 1].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![0, 1]);
    blocks.push(
        TestBlockBuilder::new()
            .transactions([2, 3].iter().map(|i| &transactions[*i]))
            .build(&mut rng)
            .into(),
    );
    block_transactions_map.push(vec![2, 3]);

    let exec_results: Vec<ExecutionResult> = (0..TRANSACTION_COUNT as u8)
        .map(|_| ExecutionResult::random(&mut rng))
        .collect();

    let mut rw_txn = test_fixture.block_store.checkout_rw().unwrap();
    for transaction in transactions.iter() {
        let _ = rw_txn.write(transaction).unwrap();
    }

    // Insert the 2 block headers and transactions into the database.
    for (block_id, block) in blocks.iter().enumerate() {
        let block_hash = rw_txn.write(&block.clone_header()).unwrap();

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

    assert!(remove_block(test_fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = test_fixture.block_store.checkout_ro().unwrap();
        let removed_block_header: Option<BlockHeader> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block_header.is_none());
        let removed_block: Option<Block> = txn.read(*blocks[0].hash()).unwrap();
        assert!(removed_block.is_none());

        let block_header: Option<BlockHeader> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block_header.is_some());
        let block: Option<Block> = txn.read(*blocks[1].hash()).unwrap();
        assert!(block.is_none());
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_missing_deploys() {
    let mut test_fixture = LmdbTestFixture::new();
    let mut rng = TestRng::new();

    let transaction = Transaction::random(&mut rng);
    let block: Block = TestBlockBuilder::new()
        .transactions(std::iter::once(&transaction))
        .build(&mut rng)
        .into();

    // Insert the block into the database.
    {
        let mut rw_txn = test_fixture.block_store.checkout_rw().unwrap();
        // Store the block.
        let _ = rw_txn.write(&block).unwrap();
        rw_txn.commit().unwrap();
    };

    assert!(remove_block(test_fixture.tmp_dir.path(), *block.hash()).is_ok());

    let txn = test_fixture.block_store.checkout_ro().unwrap();
    let maybe_transaction: Option<Transaction> = txn.read(transaction.hash()).unwrap();
    assert!(maybe_transaction.is_none());
}
