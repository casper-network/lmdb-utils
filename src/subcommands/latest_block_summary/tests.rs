use std::fs::{self, OpenOptions};

use casper_storage::block_store::{BlockStoreProvider, BlockStoreTransaction, DataWriter};
use casper_types::{testing::TestRng, Block, BlockHash, BlockHeader, TestBlockBuilder};
use once_cell::sync::Lazy;
use tempfile::{self, NamedTempFile, TempDir};

use super::block_info::BlockInfo;
use crate::{
    subcommands::latest_block_summary::{block_info, read_db},
    test_utils::LmdbTestFixture,
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
    let mut rng = TestRng::new();

    let header: BlockHeader = TestBlockBuilder::new().build(&mut rng).take_header().into();
    let block_info = BlockInfo::new(Some("casper".to_string()), header);
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
    let mut rng = TestRng::new();

    let header: BlockHeader = TestBlockBuilder::new().build(&mut rng).take_header().into();
    let block_info = BlockInfo::new(None, header);
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
    let mut rng = TestRng::new();

    let mut fixture = LmdbTestFixture::new();
    let out_file_path = OUT_DIR.as_ref().join("latest_block_metadata.json");

    // Create 2 block headers, height 0 and 1.
    let first_block: Block = TestBlockBuilder::new().height(0).build(&mut rng).into();

    let second_block: Block = TestBlockBuilder::new().height(1).build(&mut rng).into();

    // Insert the 2 blocks into the database.

    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();

    let first_block_header = first_block.take_header();
    let _ = rw_txn.write(&first_block_header).unwrap();

    let second_block_header = second_block.take_header();
    let second_block_hash = rw_txn.write(&second_block_header).unwrap();
    rw_txn.commit().unwrap();

    // Get the latest block information and ensure it matches with the second block.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let latest_block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    let second_block_info = BlockInfo::new(
        Some(
            fixture
                .tmp_dir
                .path()
                .file_name()
                .unwrap()
                .to_owned()
                .into_string()
                .unwrap(),
        ),
        second_block_header,
    );
    assert_eq!(latest_block_info, second_block_info);

    // Delete the second block from the database.
    let mut rw_txn = fixture.block_store.checkout_rw().unwrap();
    DataWriter::<BlockHash, BlockHeader>::delete(&mut rw_txn, second_block_hash).unwrap();
    rw_txn.commit().unwrap();

    // Now latest block summary should return information about the first block.
    // Given that the output exists, another run on the same destination path should fail.
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false
    )
    .is_err());
    // We use `overwrite` on the previous output file.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        true,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let latest_block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    let first_block_info = BlockInfo::new(
        Some(
            fixture
                .tmp_dir
                .path()
                .file_name()
                .unwrap()
                .to_owned()
                .into_string()
                .unwrap(),
        ),
        first_block_header,
    );
    assert_eq!(latest_block_info, first_block_info);
}

#[test]
fn latest_block_empty_db_should_fail() {
    let fixture = LmdbTestFixture::new();
    let out_file_path = OUT_DIR.as_ref().join("empty.json");
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false
    )
    .is_err());
}

#[test]
fn latest_block_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new();
    let out_file_path = OUT_DIR.as_ref().join("existing.json");
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&out_file_path)
        .unwrap();
    assert!(read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false
    )
    .is_err());
}
