use std::fs::{self, File};

use lmdb::DatabaseFlags;
use once_cell::sync::Lazy;
use tempfile::{tempdir, TempDir};

use casper_execution_engine::storage::{
    store::StoreExt,
    transaction_source::{lmdb::LmdbEnvironment, Transaction, TransactionSource},
    trie::{Pointer, PointerBlock, Trie},
    trie_store::lmdb::LmdbTrieStore,
};
use casper_hashing::Digest;
use casper_node::storage::Storage;
use casper_types::bytesrepr::{Bytes, ToBytes};

static DEFAULT_MAX_DB_SIZE: Lazy<usize> = Lazy::new(|| super::DEFAULT_MAX_DB_SIZE.parse().unwrap());

use crate::common::db::TRIE_STORE_FILE_NAME;

use super::{
    compact::{self, DestinationOptions},
    utils::{create_execution_engine, create_storage, load_execution_engine},
    Error,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TestData<K, V>(pub(crate) Digest, pub(crate) Trie<K, V>);

impl<'a, K, V> From<&'a TestData<K, V>> for (&'a Digest, &'a Trie<K, V>) {
    fn from(test_data: &'a TestData<K, V>) -> Self {
        (&test_data.0, &test_data.1)
    }
}

// Copied from `execution_engine::storage::trie_store::tests::create_data`
pub(crate) fn create_data() -> Vec<TestData<Bytes, Bytes>> {
    let leaf_1 = Trie::Leaf {
        key: Bytes::from(vec![0u8, 0, 0]),
        value: Bytes::from(b"val_1".to_vec()),
    };
    let leaf_2 = Trie::Leaf {
        key: Bytes::from(vec![1u8, 0, 0]),
        value: Bytes::from(b"val_2".to_vec()),
    };
    let leaf_3 = Trie::Leaf {
        key: Bytes::from(vec![1u8, 0, 1]),
        value: Bytes::from(b"val_3".to_vec()),
    };

    let leaf_1_hash = Digest::hash(leaf_1.to_bytes().unwrap());
    let leaf_2_hash = Digest::hash(leaf_2.to_bytes().unwrap());
    let leaf_3_hash = Digest::hash(leaf_3.to_bytes().unwrap());

    let node_2: Trie<Bytes, Bytes> = {
        let mut pointer_block = PointerBlock::new();
        pointer_block[0] = Some(Pointer::LeafPointer(leaf_2_hash));
        pointer_block[1] = Some(Pointer::LeafPointer(leaf_3_hash));
        let pointer_block = Box::new(pointer_block);
        Trie::Node { pointer_block }
    };

    let node_2_hash = Digest::hash(node_2.to_bytes().unwrap());

    let ext_node: Trie<Bytes, Bytes> = {
        let affix = vec![1u8, 0];
        let pointer = Pointer::NodePointer(node_2_hash);
        Trie::Extension {
            affix: affix.into(),
            pointer,
        }
    };

    let ext_node_hash = Digest::hash(ext_node.to_bytes().unwrap());

    let node_1: Trie<Bytes, Bytes> = {
        let mut pointer_block = PointerBlock::new();
        pointer_block[0] = Some(Pointer::LeafPointer(leaf_1_hash));
        pointer_block[1] = Some(Pointer::NodePointer(ext_node_hash));
        let pointer_block = Box::new(pointer_block);
        Trie::Node { pointer_block }
    };

    let node_1_hash = Digest::hash(node_1.to_bytes().unwrap());

    vec![
        TestData(leaf_1_hash, leaf_1),
        TestData(leaf_2_hash, leaf_2),
        TestData(leaf_3_hash, leaf_3),
        TestData(node_1_hash, node_1),
        TestData(node_2_hash, node_2),
        TestData(ext_node_hash, ext_node),
    ]
}

fn create_test_trie_store() -> (TempDir, Vec<TestData<Bytes, Bytes>>) {
    let tmp_dir = tempdir().unwrap();
    let env = LmdbEnvironment::new(tmp_dir.path(), *DEFAULT_MAX_DB_SIZE, 512, true).unwrap();
    let store = LmdbTrieStore::new(&env, None, DatabaseFlags::empty()).unwrap();
    let data = create_data();

    {
        // Put the generated data into the source trie.
        let mut txn = env.create_read_write_txn().unwrap();
        let items = data.iter().map(Into::into);
        store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    (tmp_dir, data)
}

fn create_empty_test_storage() -> (TempDir, Storage) {
    let tmp_dir = tempdir().unwrap();
    let storage = create_storage(tmp_dir.as_ref()).unwrap();
    (tmp_dir, storage)
}

#[test]
fn copy_state_root_roundtrip() {
    let src_tmp_dir = tempdir().unwrap();
    let dst_tmp_dir = tempdir().unwrap();
    let src_env =
        LmdbEnvironment::new(src_tmp_dir.path(), *DEFAULT_MAX_DB_SIZE, 512, true).unwrap();
    let src_store = LmdbTrieStore::new(&src_env, None, DatabaseFlags::empty()).unwrap();
    // Construct mock data.
    let data = create_data();

    {
        // Put the generated data into the source trie.
        let mut txn = src_env.create_read_write_txn().unwrap();
        let items = data.iter().map(Into::into);
        src_store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    let (source_state, _env) = load_execution_engine(
        src_tmp_dir.path(),
        *DEFAULT_MAX_DB_SIZE,
        Digest::default(),
        true,
    )
    .unwrap();

    let (destination_state, dst_env) =
        create_execution_engine(dst_tmp_dir.path(), *DEFAULT_MAX_DB_SIZE, true).unwrap();

    // Copy from `node1`, the root of the created trie. All data should be copied.
    super::helpers::copy_state_root(data[3].0, &source_state, &destination_state).unwrap();

    let dst_store = LmdbTrieStore::new(&dst_env, None, DatabaseFlags::empty()).unwrap();
    {
        let txn = dst_env.create_read_write_txn().unwrap();
        let keys: Vec<_> = data.iter().map(|test_data| test_data.0).collect();
        let entries: Vec<Option<Trie<Bytes, Bytes>>> =
            dst_store.get_many(&txn, keys.iter()).unwrap();
        for entry in entries {
            match entry {
                Some(trie) => {
                    let trie_in_data = data.iter().find(|test_data| test_data.1 == trie);
                    // Check we are not missing anything since all data should be copied.
                    assert!(trie_in_data.is_some());
                    // Hashes should be equal.
                    assert_eq!(
                        trie_in_data.unwrap().0,
                        Digest::hash(trie.to_bytes().unwrap())
                    );
                }
                None => panic!(),
            }
        }
        txn.commit().unwrap();
    }

    src_tmp_dir.close().unwrap();
    dst_tmp_dir.close().unwrap();
}

#[test]
fn check_no_extra_tries() {
    let src_tmp_dir = tempdir().unwrap();
    let dst_tmp_dir = tempdir().unwrap();
    let src_env =
        LmdbEnvironment::new(src_tmp_dir.path(), *DEFAULT_MAX_DB_SIZE, 512, true).unwrap();
    let src_store = LmdbTrieStore::new(&src_env, None, DatabaseFlags::empty()).unwrap();
    // Construct mock data.
    let data = create_data();

    {
        // Put the generated data into the source trie.
        let mut txn = src_env.create_read_write_txn().unwrap();
        let items = data.iter().map(Into::into);
        src_store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    let (source_state, _env) = load_execution_engine(
        src_tmp_dir.path(),
        *DEFAULT_MAX_DB_SIZE,
        Digest::default(),
        true,
    )
    .unwrap();

    let (destination_state, dst_env) =
        create_execution_engine(dst_tmp_dir.path(), *DEFAULT_MAX_DB_SIZE, true).unwrap();

    // Check with `node2`, which only has `leaf1` and `leaf2` as children in the constructed trie.
    super::helpers::copy_state_root(data[4].0, &source_state, &destination_state).unwrap();

    let dst_store = LmdbTrieStore::new(&dst_env, None, DatabaseFlags::empty()).unwrap();
    {
        let txn = dst_env.create_read_write_txn().unwrap();
        let data_keys: Vec<_> = data.iter().map(|test_data| test_data.0).collect();
        // `TestData` objects `[leaf2, leaf3, node2]` which should be included in the search result.
        let mut included_data = vec![data[1].clone(), data[2].clone(), data[4].clone()];
        let entries: Vec<Option<Trie<Bytes, Bytes>>> =
            dst_store.get_many(&txn, data_keys.iter()).unwrap();
        // Get rid of the empty entries and count them.
        let mut miss_count = 0usize;
        let entries: Vec<Trie<Bytes, Bytes>> = entries
            .iter()
            .filter_map(|maybe_trie| match maybe_trie {
                Some(trie) => Some(trie.clone()),
                None => {
                    miss_count += 1;
                    None
                }
            })
            .collect();
        // Make sure we missed the correct amount of entries.
        assert_eq!(miss_count, data.len() - included_data.len());
        // Construct `TestData` from our `Trie`s.
        let mut entries: Vec<TestData<_, _>> = entries
            .iter()
            .map(|trie| TestData(Digest::hash(trie.to_bytes().unwrap()), trie.clone()))
            .collect();

        // Ensure we got exactly the right data back from the destination trie store.
        // We sort for the convenience of using `assert_eq` with `Vec`s directly.
        included_data.sort_by_key(|test_data| test_data.0);
        entries.sort_by_key(|test_data| test_data.0);
        assert_eq!(included_data, entries);

        txn.commit().unwrap();
    }

    src_tmp_dir.close().unwrap();
    dst_tmp_dir.close().unwrap();
}

#[test]
fn missing_source_trie() {
    match compact::trie_compact(
        "",
        "bogus_path",
        "",
        DestinationOptions::New,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidPath(..)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }
}

#[test]
fn missing_storage() {
    let (src_dir, _) = create_test_trie_store();
    let dst_dir = tempdir().unwrap();
    match compact::trie_compact(
        "bogus_path",
        src_dir,
        dst_dir,
        DestinationOptions::New,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::OpenStorage(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }
}

#[test]
fn valid_empty_dst_with_destination_options() {
    let (src_dir, _) = create_test_trie_store();
    let dst_dir = tempdir().unwrap();
    let (storage_dir, _store) = create_empty_test_storage();
    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::New,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Ok(_) => {}
        Err(err) => panic!("Unexpected error: {err}"),
    }
    fs::remove_file(dst_dir.path().join(TRIE_STORE_FILE_NAME)).unwrap();

    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Append,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidDest(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }

    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Overwrite,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidDest(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }
}

#[test]
fn valid_existing_dst_with_destination_options() {
    let (src_dir, _) = create_test_trie_store();
    let dst_dir = tempdir().unwrap();
    {
        let _dst_trie_file = File::create(dst_dir.path().join(TRIE_STORE_FILE_NAME)).unwrap();
        assert!(dst_dir.path().join(TRIE_STORE_FILE_NAME).exists())
    }

    let (storage_dir, _store) = create_empty_test_storage();
    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::New,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidDest(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }

    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Append,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Ok(_) => {}
        Err(err) => panic!("Unexpected error: {err}"),
    }

    assert!(dst_dir.path().join(TRIE_STORE_FILE_NAME).exists());
    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Overwrite,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Ok(_) => {}
        Err(err) => panic!("Unexpected error: {err}"),
    }

    fs::remove_file(dst_dir.path().join(TRIE_STORE_FILE_NAME))
        .expect("Couldn't delete mock destination data.lmdb");
}

#[test]
fn missing_dst_with_destination_options() {
    let (src_dir, _) = create_test_trie_store();
    let root_dst_dir = tempdir().unwrap();
    let dst_dir = root_dst_dir.path().join("extra_dir");
    let (storage_dir, _store) = create_empty_test_storage();
    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::New,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Ok(_) => {}
        Err(err) => panic!("Unexpected error: {err}"),
    }
    fs::remove_dir_all(dst_dir.as_path()).unwrap();

    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Append,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidDest(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }

    match compact::trie_compact(
        &storage_dir,
        &src_dir,
        &dst_dir,
        DestinationOptions::Overwrite,
        *DEFAULT_MAX_DB_SIZE,
    ) {
        Err(Error::InvalidDest(_)) => {}
        Err(err) => panic!("Unexpected error: {err}"),
        Ok(_) => panic!("Unexpected successful trie compact"),
    }
}
