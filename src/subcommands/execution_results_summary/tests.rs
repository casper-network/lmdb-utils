use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    slice,
};

use casper_node::types::{BlockHash, DeployHash};
use casper_types::bytesrepr::ToBytes;
use lmdb::{Transaction, WriteFlags};
use once_cell::sync::Lazy;
use rand::Rng;
use tempfile::{self, TempDir};

use crate::{
    common::db::{Database, DeployMetadataDatabase, STORAGE_FILE_NAME},
    subcommands::execution_results_summary::{
        block_body::BlockBody,
        read_db,
        summary::{
            chunk_count_after_partition, summarize_map, CollectionStatistics,
            ExecutionResultsStats, ExecutionResultsSummary, CHUNK_SIZE_BYTES,
        },
        Error,
    },
    test_utils::{self, LmdbTestFixture, MockBlockHeader},
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
    let mut stats = ExecutionResultsStats::default();
    let mut bincode_sizes = vec![];
    let mut bytesrepr_sizes = vec![];

    for i in 1..4 {
        let mut execution_results = vec![];
        for _ in 0..(10 * i) {
            execution_results.push(test_utils::success_execution_result());
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
    let mut stats = ExecutionResultsStats::default();
    let mut bincode_sizes = vec![];
    let mut bytesrepr_sizes = vec![];

    for _ in 1..4 {
        let mut execution_results = vec![];
        for _ in 0..10 {
            execution_results.push(test_utils::success_execution_result());
        }
        bincode_sizes.push(bincode::serialized_size(&execution_results).unwrap() as usize);
        bytesrepr_sizes.push(chunk_count_after_partition(
            execution_results.serialized_length(),
        ));
        stats.feed(execution_results).unwrap();
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
    const BLOCK_COUNT: usize = 3;
    const DEPLOY_COUNT: usize = 4;

    let fixture = LmdbTestFixture::new(
        vec!["block_header", "block_body", "deploy_metadata"],
        Some(STORAGE_FILE_NAME),
    );
    let out_file_path = OUT_DIR.as_ref().join("execution_results_summary.json");

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8)
        .map(test_utils::mock_deploy_hash)
        .collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    let mut block_bodies = vec![];
    let mut block_body_deploy_map: Vec<Vec<usize>> = vec![];
    block_bodies.push(BlockBody::new(vec![
        deploy_hashes[0],
        deploy_hashes[1],
        deploy_hashes[3],
    ]));
    block_body_deploy_map.push(vec![0, 1, 3]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[1], deploy_hashes[2]]));
    block_body_deploy_map.push(vec![1, 2]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[2], deploy_hashes[3]]));
    block_body_deploy_map.push(vec![2, 3]);

    let deploy_metadatas = [
        test_utils::mock_deploy_metadata(slice::from_ref(&block_headers[0].0)),
        test_utils::mock_deploy_metadata(&[block_headers[0].0, block_headers[1].0]),
        test_utils::mock_deploy_metadata(&[block_headers[1].0, block_headers[2].0]),
        test_utils::mock_deploy_metadata(&[block_headers[0].0, block_headers[2].0]),
    ];

    let env = &fixture.env;
    // Insert the 3 blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for i in 0..BLOCK_COUNT {
            // Store the header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the body.
            txn.put(
                *fixture.db(Some("block_body")).unwrap(),
                &block_headers[i].1.body_hash,
                &bincode::serialize(&block_bodies[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the 4 deploys into the database.
        for i in 0..DEPLOY_COUNT {
            txn.put(
                *fixture.db(Some("deploy_metadata")).unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

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
    for (block_idx, (block_hash, _block_header)) in block_headers.iter().enumerate() {
        let _block_body = &block_bodies[block_idx];
        let mut execution_results = vec![];
        for metadata_idx in &block_body_deploy_map[block_idx] {
            execution_results.push(
                deploy_metadatas[*metadata_idx]
                    .execution_results
                    .get(block_hash)
                    .unwrap()
                    .clone(),
            );
        }
        stats.feed(execution_results).unwrap();
    }
    let expected_summary: ExecutionResultsSummary = stats.into();
    assert_eq!(execution_results_summary, expected_summary);
}

#[test]
fn execution_results_summary_invalid_key_should_fail() {
    let fixture = LmdbTestFixture::new(
        vec!["block_header", "block_body", "deploy_metadata"],
        Some(STORAGE_FILE_NAME),
    );
    let out_file_path = OUT_DIR.as_ref().join("invalid_key.json");

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        let (_, block_header) = test_utils::mock_block_header(0);
        let bogus_hash = [0u8; 1];
        // Insert a block header in the database with a key that can't be
        // deserialized.
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &bogus_hash,
            &bincode::serialize(&block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::InvalidKey(idx)) => assert_eq!(idx, 0),
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}

#[test]
fn execution_results_summary_parsing_should_fail() {
    let fixture = LmdbTestFixture::new(
        vec!["block_header", "block_body", "deploy_metadata"],
        Some(STORAGE_FILE_NAME),
    );
    let out_file_path = OUT_DIR.as_ref().join("parsing.json");

    let deploy_hash = test_utils::mock_deploy_hash(0);
    let (block_hash, block_header) = test_utils::mock_block_header(0);
    let block_body = BlockBody::new(vec![deploy_hash]);

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the header.
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &block_hash,
            &bincode::serialize(&block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the body.
        txn.put(
            *fixture.db(Some("block_body")).unwrap(),
            &block_header.body_hash,
            &bincode::serialize(&block_body).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store a bogus metadata under the deploy hash key we used before.
        txn.put(
            *fixture.db(Some("deploy_metadata")).unwrap(),
            &deploy_hash,
            &"bogus_deploy_metadata".to_bytes().unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::Parsing(hash, db_name, _bincode_err)) => {
            assert_eq!(hash, block_hash);
            assert_eq!(db_name, DeployMetadataDatabase::db_name());
        }
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}

#[test]
fn execution_results_summary_bogus_db_should_fail() {
    let fixture = LmdbTestFixture::new(vec!["bogus"], Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("bogus_db.json");

    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::Database(_)) => { /* expected result */ }
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}

#[test]
fn execution_results_summary_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new(
        vec!["block_header", "block_body", "deploy_metadata"],
        Some(STORAGE_FILE_NAME),
    );
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
