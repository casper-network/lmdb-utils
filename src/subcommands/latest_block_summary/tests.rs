use std::fs::{self, OpenOptions};

use lmdb::{Transaction, WriteFlags};
use once_cell::sync::Lazy;
use tempfile::{self, NamedTempFile, TempDir};

use casper_node::{
    rpcs::docs::DocExample,
    types::{BlockHeader, JsonBlockHeader},
};

use super::block_info::BlockInfo;
use crate::{
    common::db::STORAGE_FILE_NAME,
    subcommands::latest_block_summary::{block_info, read_db},
    test_utils::{LmdbTestFixture, MockBlockHeader},
};

static OUT_DIR: Lazy<TempDir> = Lazy::new(|| tempfile::tempdir().unwrap());

#[test]
fn parse_network_name_input() {
    let root_dir = tempfile::tempdir().unwrap();
    let first_node = tempfile::tempdir_in(&root_dir).unwrap();
    let second_node = tempfile::tempdir_in(&first_node).unwrap();
    let file = NamedTempFile::new_in(first_node.as_ref()).unwrap();

    assert_eq!(
        block_info::parse_network_name(&second_node).unwrap(),
        second_node.path().file_name().unwrap().to_str().unwrap()
    );
    assert_eq!(
        block_info::parse_network_name(&first_node).unwrap(),
        first_node.path().file_name().unwrap().to_str().unwrap()
    );
    let relative_path_to_first_node = second_node.as_ref().join("..");
    assert_eq!(
        block_info::parse_network_name(relative_path_to_first_node).unwrap(),
        first_node.path().file_name().unwrap().to_str().unwrap()
    );

    assert!(block_info::parse_network_name("/").is_err());
    assert!(block_info::parse_network_name(file.path()).is_err());
}

#[test]
fn dump_with_net_name() {
    let json_header = JsonBlockHeader::doc_example().clone();
    let header: BlockHeader = json_header.into();
    let block_info = BlockInfo::new(Some("casper".to_string()), header.hash(), header);
    let reference_json = serde_json::to_string_pretty(&block_info).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("casper_network.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_block_info(&block_info, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn dump_without_net_name() {
    let json_header = JsonBlockHeader::doc_example().clone();
    let header: BlockHeader = json_header.into();
    let block_info = BlockInfo::new(None, header.hash(), header);
    let reference_json = serde_json::to_string_pretty(&block_info).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("no_net_name.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_block_info(&block_info, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn latest_block_should_succeed() {
    let fixture = LmdbTestFixture::new(vec!["block_header"], Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("latest_block_metadata.json");

    // Create 2 block headers, height 0 and 1.
    let first_block = MockBlockHeader::default();
    let first_block_key = [0u8; 32];

    let mut second_block = MockBlockHeader::default();
    let second_block_key = [1u8; 32];
    second_block.height = 1;

    let env = &fixture.env;
    let db = fixture.db(Some("block_header")).unwrap();
    // Insert the 2 blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        txn.put(
            *db,
            &first_block_key,
            &bincode::serialize(&first_block).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.put(
            *db,
            &second_block_key,
            &bincode::serialize(&second_block).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    // Get the latest block information and ensure it matches with the second block.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    let (mock_block_header_deserialized, _network_name) = block_info.into_mock();
    assert_eq!(mock_block_header_deserialized, second_block);

    // Delete the second block from the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        txn.del(*db, &second_block_key, None).unwrap();
        txn.commit().unwrap();
    };

    // Now latest block summary should return information about the first block.
    // Given that the output exists, another run on the same destination path should fail.
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
        false,
    )
    .is_err());
    // We use `overwrite` on the previous output file.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        true,
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    let (mock_block_header_deserialized, _network_name) = block_info.into_mock();
    assert_eq!(mock_block_header_deserialized, first_block);
}

#[test]
fn latest_block_empty_db_should_fail() {
    let fixture = LmdbTestFixture::new(vec!["block_header_faulty"], Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("empty.json");
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
        false,
    )
    .is_err());
}

#[test]
fn latest_block_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new(vec!["block_header_faulty"], Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("existing.json");
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&out_file_path)
        .unwrap();
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
        false,
    )
    .is_err());
}
