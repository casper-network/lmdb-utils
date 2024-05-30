use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
};

use casper_storage::block_store::{
    types::{BlockExecutionResults, BlockHashHeightAndEra},
    BlockStoreProvider, BlockStoreTransaction, DataWriter,
};
use casper_types::bytesrepr::ToBytes;
use casper_types::{
    execution::ExecutionResult, testing::TestRng, Block, TestBlockBuilder, Transaction,
};
use once_cell::sync::Lazy;
use rand::Rng;
use tempfile::{self, TempDir};

use crate::{
    subcommands::execution_results_summary::{
        read_db,
        summary::{
            chunk_count_after_partition, summarize_map, CollectionStatistics,
            ExecutionResultsStats, ExecutionResultsSummary, CHUNK_SIZE_BYTES,
        },
        Error,
    },
    test_utils::{self, LmdbTestFixture},
};

static OUT_DIR: Lazy<TempDir> = Lazy::new(|| tempfile::tempdir().unwrap());

#[test]
fn check_chunk_count_after_partition() {
    assert_eq!(chunk_count_after_partition(0), 0);
    assert_eq!(chunk_count_after_partition(1), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES / 2), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES - 1), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES + 1), 2);
    assert_eq!(chunk_count_after_partition((CHUNK_SIZE_BYTES * 3) / 2), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES - 1), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES + 1), 3);
}

#[test]
fn check_summarize_map() {
    // Empty map.
    assert_eq!(
        summarize_map(&BTreeMap::default()),
        CollectionStatistics::default()
    );

    // 1 element map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.0, 1, 1));

    // 2 different elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    map.insert(2, 1);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.5, 2, 2));

    // 2 identical elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.0, 1, 1));

    // 3 elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    map.insert(4, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(3.0, 4, 4));

    // 10 elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 2);
    map.insert(3, 2);
    map.insert(4, 4);
    map.insert(8, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(4.0, 4, 8));
}

#[test]
fn check_summarize_map_random() {
    let mut rng = rand::thread_rng();
    let elem_count = rng.gen_range(50usize..100usize);
    let mut elements: Vec<usize> = vec![];
    let mut sum = 0;
    for _ in 0..elem_count {
        let random_element = rng.gen_range(0usize..25usize);
        sum += random_element;
        elements.push(random_element);
    }
    elements.sort_unstable();
    let median = elements[elem_count / 2];
    let max = *elements.last().unwrap();
    let average = sum as f64 / elem_count as f64;

    let mut map = BTreeMap::default();
    for element in elements {
        if let Some(count) = map.get_mut(&element) {
            *count += 1;
        } else {
            map.insert(element, 1);
        }
    }
    assert_eq!(
        summarize_map(&map),
        CollectionStatistics::new(average, median, max)
    );
}

#[test]
fn dump_execution_results_summary() {
    let mut stats = ExecutionResultsStats::default();
    stats.execution_results_size.insert(1, 2);
    stats.chunk_count.insert(1, 1);
    stats.chunk_count.insert(2, 1);
    let summary: ExecutionResultsSummary = stats.into();
    let reference_json = serde_json::to_string_pretty(&summary).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("no_net_name.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_execution_results_summary(&summary, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn empty_execution_results_stats() {
    let stats = ExecutionResultsStats::default();
    let summary: ExecutionResultsSummary = stats.into();
    assert_eq!(summary.execution_results_size.average, 0.0);
    assert_eq!(summary.execution_results_size.median, 0);
    assert_eq!(summary.execution_results_size.max, 0);

    assert_eq!(summary.chunks_statistics.average, 0.0);
    assert_eq!(summary.chunks_statistics.median, 0);
    assert_eq!(summary.chunks_statistics.max, 0);
}

#[test]
fn different_execution_results_stats_feed() {
    let mut rng = TestRng::new();
    let mut stats = ExecutionResultsStats::default();
    let mut bincode_sizes = vec![];
    let mut bytesrepr_sizes = vec![];

    for i in 1..4 {
        let mut execution_results = vec![];
        for _ in 0..(10 * i) {
            execution_results.push(test_utils::success_execution_result(&mut rng));
        }
        bincode_sizes.push(bincode::serialized_size(&execution_results).unwrap() as usize);
        bytesrepr_sizes.push(chunk_count_after_partition(
            execution_results.serialized_length(),
        ));
        stats.feed(execution_results).unwrap();
    }

    let summary: ExecutionResultsSummary = stats.into();

    let bincode_sizes_average: f64 = bincode_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(
        summary.execution_results_size.average,
        bincode_sizes_average
    );
    assert_eq!(summary.execution_results_size.median, bincode_sizes[1]);
    assert_eq!(summary.execution_results_size.max, bincode_sizes[2]);

    let bytesrepr_sizes_average: f64 = bytesrepr_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(summary.chunks_statistics.average, bytesrepr_sizes_average);
    assert_eq!(summary.chunks_statistics.median, bytesrepr_sizes[1]);
    assert_eq!(summary.chunks_statistics.max, bytesrepr_sizes[2]);
}

#[test]
fn identical_execution_results_stats_feed() {
    let mut rng = TestRng::new();
    let mut stats = ExecutionResultsStats::default();
    let mut bincode_sizes = vec![];
    let mut bytesrepr_sizes = vec![];

    let mut execution_results = vec![];
    for _ in 0..10 {
        execution_results.push(test_utils::success_execution_result(&mut rng));
    }

    for _ in 1..4 {
        bincode_sizes.push(bincode::serialized_size(&execution_results).unwrap() as usize);
        bytesrepr_sizes.push(chunk_count_after_partition(
            execution_results.serialized_length(),
        ));
        stats.feed(execution_results.clone()).unwrap();
    }
    assert_eq!(stats.execution_results_size.len(), 1);
    assert_eq!(stats.chunk_count.len(), 1);

    let summary: ExecutionResultsSummary = stats.into();

    let bincode_sizes_average: f64 = bincode_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(
        summary.execution_results_size.average,
        bincode_sizes_average
    );
    assert_eq!(summary.execution_results_size.median, bincode_sizes[1]);
    assert_eq!(summary.execution_results_size.max, bincode_sizes[2]);
    assert_eq!(
        summary.execution_results_size.median,
        summary.execution_results_size.max
    );

    let bytesrepr_sizes_average: f64 = bytesrepr_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(summary.chunks_statistics.average, bytesrepr_sizes_average);
    assert_eq!(summary.chunks_statistics.median, bytesrepr_sizes[1]);
    assert_eq!(summary.chunks_statistics.max, bytesrepr_sizes[2]);
    assert_eq!(
        summary.chunks_statistics.median,
        summary.chunks_statistics.max
    );
}

#[test]
fn execution_results_stats_should_succeed() {
    const TRANSACTION_COUNT: usize = 7;

    let mut rng = TestRng::new();

    let mut fixture = LmdbTestFixture::new();
    let out_file_path = OUT_DIR.as_ref().join("execution_results_summary.json");

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

    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();

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

    // Get the execution results summary and ensure it matches with the
    // expected statistics.
    read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let execution_results_summary: ExecutionResultsSummary =
        serde_json::from_str(&json_str).unwrap();

    // Construct the expected statistics.
    let mut stats = ExecutionResultsStats::default();
    for (block_idx, _block) in blocks.iter().enumerate() {
        let mut execution_results = vec![];
        for exec_result_idx in &block_transactions_map[block_idx] {
            execution_results.push(exec_results[*exec_result_idx].clone());
        }
        stats.feed(execution_results).unwrap();
    }
    let expected_summary: ExecutionResultsSummary = stats.into();
    assert_eq!(execution_results_summary, expected_summary);
}

#[test]
fn execution_results_summary_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new();
    let out_file_path = OUT_DIR.as_ref().join("existing.json");
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&out_file_path)
        .unwrap();
    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::Output(_)) => { /* expected result */ }
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}
