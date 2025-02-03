use std::{
    fs::{self, File},
    path::Path,
};

use once_cell::sync::Lazy;
use rand::{self, RngCore};
use tar::Archive;
use tempfile::{NamedTempFile, TempDir};
use zstd::Decoder;

use crate::subcommands::archive::{create::pack, zstd_utils::WINDOW_LOG_MAX_SIZE};

const NUM_TEST_FILES: usize = 10usize;
const TEST_FILE_SIZE: usize = 10000usize;

static MOCK_DIR: Lazy<(TempDir, TestPayloads)> = Lazy::new(create_mock_src_dir);

struct TestPayloads {
    pub payloads: [[u8; TEST_FILE_SIZE]; NUM_TEST_FILES],
}

fn create_mock_src_dir() -> (TempDir, TestPayloads) {
    let src_dir = tempfile::tempdir().unwrap();

    let mut rng = rand::thread_rng();
    let mut payloads = [[0u8; TEST_FILE_SIZE]; NUM_TEST_FILES];
    for (idx, payload) in payloads.iter_mut().enumerate().take(NUM_TEST_FILES) {
        rng.fill_bytes(payload);
        fs::write(src_dir.path().join(format!("file_{idx}")), &payload).unwrap();
    }
    (src_dir, TestPayloads { payloads })
}

fn unpack_mock_archive<P1: AsRef<Path>, P2: AsRef<Path>>(archive_path: P1, dst_dir: P2) {
    let archive_file = File::open(&archive_path).unwrap();
    let mut decoder = Decoder::new(archive_file).unwrap();
    decoder.window_log_max(WINDOW_LOG_MAX_SIZE).unwrap();
    let mut unpacker = Archive::new(decoder);
    unpacker.unpack(&dst_dir).unwrap();
    fs::remove_file(&archive_path).unwrap();
}

#[test]
fn archive_create_roundtrip() {
    // Create the mock test directory with randomly-filled files.
    let src_dir = &MOCK_DIR.0;
    let test_payloads = &MOCK_DIR.1;
    let dst_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let archive_path = dst_dir.path().join("test_archive.tar.zst");
    // Create the compressed archive.
    assert!(pack::create_archive(src_dir, &archive_path, false).is_ok());
    // Unpack and then delete the archive.
    unpack_mock_archive(&archive_path, &out_dir);
    for idx in 0..NUM_TEST_FILES {
        let contents = fs::read(out_dir.path().join(format!("file_{idx}"))).unwrap();
        if contents != test_payloads.payloads[idx] {
            panic!("Contents of file {idx} are different from the original");
        }
    }
}

#[test]
fn archive_create_overwrite() {
    // Create the mock test directory with randomly-filled files.
    let src_dir = &MOCK_DIR.0;
    let test_payloads = &MOCK_DIR.1;
    let dst_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let archive_path = dst_dir.path().join("test_archive.tar.zst");
    // Create a mock existing file at the expected destination.
    fs::write(&archive_path, "dummy input").unwrap();
    // File already exists, so creating the archive without the overwrite flag
    // should fail.
    assert!(pack::create_archive(src_dir, &archive_path, false).is_err());
    // Create the compressed archive with the overwrite set.
    assert!(pack::create_archive(src_dir, &archive_path, true).is_ok());
    // Unpack and then delete the archive.
    unpack_mock_archive(&archive_path, &out_dir);
    for idx in 0..NUM_TEST_FILES {
        let contents = fs::read(out_dir.path().join(format!("file_{idx}"))).unwrap();
        if contents != test_payloads.payloads[idx] {
            panic!("Contents of file {idx} are different from the original");
        }
    }
}

#[test]
fn archive_create_bad_input() {
    let src_dir = &MOCK_DIR.0;
    let root_dst = tempfile::tempdir().unwrap();
    let inexistent_file_path = root_dst.path().join("bogus_path");

    // Source doesn't exist.
    assert!(pack::create_archive(&inexistent_file_path, &inexistent_file_path, false).is_err());

    // Source is not a directory.
    let file = NamedTempFile::new().unwrap();
    assert!(pack::create_archive(file.path(), &inexistent_file_path, false).is_err());

    // Destination directory doesn't exist.
    let root_dst = tempfile::tempdir().unwrap();
    assert!(pack::create_archive(
        src_dir,
        root_dst.path().join("bogus_dest/test_archive.tar.zst"),
        false,
    )
    .is_err());

    // Destination directory isn't empty.
    let root_dst = tempfile::tempdir().unwrap();
    let existing_file = NamedTempFile::new_in(&root_dst).unwrap();
    assert!(pack::create_archive(src_dir, existing_file.path(), false).is_err());
}
