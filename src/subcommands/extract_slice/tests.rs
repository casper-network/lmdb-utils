use std::slice;

use casper_execution_engine::storage::{
    store::StoreExt,
    transaction_source::{lmdb::LmdbEnvironment, TransactionSource},
    trie::Trie,
    trie_store::lmdb::LmdbTrieStore,
};
use casper_hashing::Digest;
use casper_node::types::{BlockHash, DeployHash, DeployMetadata};
use casper_types::bytesrepr::{Bytes, ToBytes};
use lmdb::{DatabaseFlags, Error as LmdbError, Transaction, WriteFlags};

use crate::{
    common::db::{
        BlockBodyDatabase, BlockHeaderDatabase, Database, DeployDatabase, DeployMetadataDatabase,
        TransferDatabase, STORAGE_FILE_NAME,
    },
    subcommands::{
        execution_results_summary::block_body::BlockBody,
        extract_slice::{db_helpers, global_state, storage},
        trie_compact::{
            create_execution_engine, load_execution_engine, tests::create_data, DEFAULT_MAX_DB_SIZE,
        },
    },
    test_utils::{
        mock_block_header, mock_deploy_hash, mock_deploy_metadata, LmdbTestFixture, MockBlockHeader,
    },
};

#[test]
fn transfer_data_between_dbs() {
    const DATA_COUNT: usize = 4;
    const MOCK_DB_NAME: &str = "mock_data";

    let source_fixture = LmdbTestFixture::new(vec![MOCK_DB_NAME], Some(STORAGE_FILE_NAME));

    let deploy_hashes: Vec<DeployHash> = (0..DATA_COUNT as u8).map(mock_deploy_hash).collect();

    {
        let env = &source_fixture.env;
        // Insert the 3 blocks into the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            for (i, deploy_hash) in deploy_hashes.iter().enumerate().take(DATA_COUNT) {
                txn.put(
                    *source_fixture.db(Some(MOCK_DB_NAME)).unwrap(),
                    &i.to_le_bytes(),
                    &bincode::serialize(deploy_hash).unwrap(),
                    WriteFlags::empty(),
                )
                .unwrap();
            }
            txn.commit().unwrap();
        };
    }

    let destination_fixture = LmdbTestFixture::new(vec![MOCK_DB_NAME], Some(STORAGE_FILE_NAME));

    {
        let mut source_txn = source_fixture.env.begin_ro_txn().unwrap();
        assert_eq!(
            db_helpers::read_from_db(&mut source_txn, MOCK_DB_NAME, &0usize.to_le_bytes()).unwrap(),
            bincode::serialize(&deploy_hashes[0]).unwrap()
        );
        assert_eq!(
            db_helpers::read_from_db(&mut source_txn, MOCK_DB_NAME, &DATA_COUNT.to_le_bytes())
                .unwrap_err(),
            LmdbError::NotFound
        );
        source_txn.commit().unwrap();
    }

    {
        let mut destination_txn = destination_fixture.env.begin_rw_txn().unwrap();
        let serialized_deploy_hash = bincode::serialize(&deploy_hashes[1]).unwrap();
        assert!(db_helpers::write_to_db(
            &mut destination_txn,
            MOCK_DB_NAME,
            &1usize.to_le_bytes(),
            &serialized_deploy_hash
        )
        .is_ok());
        destination_txn.commit().unwrap();
    }

    {
        let mut source_txn = source_fixture.env.begin_ro_txn().unwrap();
        let mut destination_txn = destination_fixture.env.begin_rw_txn().unwrap();
        let serialized_deploy_hash = bincode::serialize(&deploy_hashes[2]).unwrap();
        let copied_bytes = db_helpers::transfer_to_new_db(
            &mut source_txn,
            &mut destination_txn,
            MOCK_DB_NAME,
            &2usize.to_le_bytes(),
        )
        .unwrap();
        assert_eq!(serialized_deploy_hash, copied_bytes);
        assert_eq!(
            db_helpers::transfer_to_new_db(
                &mut source_txn,
                &mut destination_txn,
                MOCK_DB_NAME,
                &DATA_COUNT.to_le_bytes()
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        source_txn.commit().unwrap();
        destination_txn.commit().unwrap();
    }

    {
        let destination_txn = destination_fixture.env.begin_ro_txn().unwrap();
        let destination_db = destination_fixture.db(Some(MOCK_DB_NAME)).unwrap();
        assert_eq!(
            destination_txn
                .get(*destination_db, &0usize.to_le_bytes())
                .unwrap_err(),
            LmdbError::NotFound
        );
        assert_eq!(
            destination_txn
                .get(*destination_db, &1usize.to_le_bytes())
                .unwrap(),
            bincode::serialize(&deploy_hashes[1]).unwrap()
        );
        assert_eq!(
            destination_txn
                .get(*destination_db, &2usize.to_le_bytes())
                .unwrap(),
            bincode::serialize(&deploy_hashes[2]).unwrap()
        );
        assert_eq!(
            destination_txn
                .get(*destination_db, &DATA_COUNT.to_le_bytes())
                .unwrap_err(),
            LmdbError::NotFound
        );
        destination_txn.commit().unwrap();
    }
}

#[test]
fn transfer_blocks() {
    const BLOCK_COUNT: usize = 3;
    const DEPLOY_COUNT: usize = 4;

    let source_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
            DeployDatabase::db_name(),
            TransferDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8).map(mock_deploy_hash).collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> =
        (0..BLOCK_COUNT as u8).map(mock_block_header).collect();
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
        mock_deploy_metadata(slice::from_ref(&block_headers[0].0)),
        mock_deploy_metadata(&[block_headers[0].0, block_headers[1].0]),
        mock_deploy_metadata(&[block_headers[1].0, block_headers[2].0]),
        mock_deploy_metadata(&[block_headers[0].0, block_headers[2].0]),
    ];

    let env = &source_fixture.env;
    // Insert the 3 blocks into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for i in 0..BLOCK_COUNT {
            // Store the header.
            txn.put(
                *source_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the body.
            txn.put(
                *source_fixture
                    .db(Some(BlockBodyDatabase::db_name()))
                    .unwrap(),
                &block_headers[i].1.body_hash,
                &bincode::serialize(&block_bodies[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the 4 deploys into the deploys and deploy_metadata databases.
        for i in 0..DEPLOY_COUNT {
            txn.put(
                *source_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Add mock deploy data in the database.
            txn.put(
                *source_fixture.db(Some(DeployDatabase::db_name())).unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_hashes[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let destination_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
            DeployDatabase::db_name(),
            TransferDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let block_hash_0 = block_headers[0].0;
    let expected_state_root_hash = block_headers[0].1.state_root_hash;
    let actual_state_root_hash = storage::transfer_block_info(
        source_fixture.tmp_dir.path(),
        destination_fixture.tmp_dir.path(),
        block_hash_0,
    )
    .unwrap();
    assert_eq!(expected_state_root_hash, actual_state_root_hash);

    {
        let txn = destination_fixture.env.begin_ro_txn().unwrap();
        let actual_block_header: MockBlockHeader = txn
            .get(
                *destination_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_hash_0,
            )
            .map(bincode::deserialize)
            .unwrap()
            .unwrap();
        assert_eq!(actual_block_header, block_headers[0].1);

        let actual_block_body: BlockBody = txn
            .get(
                *destination_fixture
                    .db(Some(BlockBodyDatabase::db_name()))
                    .unwrap(),
                &actual_block_header.body_hash,
            )
            .map(bincode::deserialize)
            .unwrap()
            .unwrap();
        assert_eq!(actual_block_body, block_bodies[0]);

        for deploy_hash in actual_block_body.deploy_hashes() {
            let actual_mock_deploy: DeployHash = txn
                .get(
                    *destination_fixture
                        .db(Some(DeployDatabase::db_name()))
                        .unwrap(),
                    deploy_hash,
                )
                .map(bincode::deserialize)
                .unwrap()
                .unwrap();
            assert_eq!(*deploy_hash, actual_mock_deploy);

            let mut actual_deploy_metadata: DeployMetadata = txn
                .get(
                    *destination_fixture
                        .db(Some(DeployMetadataDatabase::db_name()))
                        .unwrap(),
                    deploy_hash,
                )
                .map(bincode::deserialize)
                .unwrap()
                .unwrap();
            assert!(actual_deploy_metadata
                .execution_results
                .remove(&block_hash_0)
                .is_some());
            assert!(actual_deploy_metadata.execution_results.is_empty());
        }

        assert_eq!(
            txn.get(
                *destination_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[1].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert_eq!(
            txn.get(
                *destination_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[2].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        txn.commit().unwrap();
    }

    let block_hash_1 = block_headers[1].0;

    // Put some mock data in the transfer DB under block hash 1.
    {
        let mut txn = source_fixture.env.begin_rw_txn().unwrap();
        txn.put(
            *source_fixture
                .db(Some(TransferDatabase::db_name()))
                .unwrap(),
            &block_hash_1,
            &bincode::serialize(&block_hash_1).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let expected_state_root_hash = block_headers[1].1.state_root_hash;
    let actual_state_root_hash = storage::transfer_block_info(
        source_fixture.tmp_dir.path(),
        destination_fixture.tmp_dir.path(),
        block_hash_1,
    )
    .unwrap();
    assert_eq!(expected_state_root_hash, actual_state_root_hash);

    {
        let txn = destination_fixture.env.begin_ro_txn().unwrap();
        let actual_block_header: MockBlockHeader = txn
            .get(
                *destination_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_hash_1,
            )
            .map(bincode::deserialize)
            .unwrap()
            .unwrap();
        assert_eq!(actual_block_header, block_headers[1].1);

        let actual_block_body: BlockBody = txn
            .get(
                *destination_fixture
                    .db(Some(BlockBodyDatabase::db_name()))
                    .unwrap(),
                &actual_block_header.body_hash,
            )
            .map(bincode::deserialize)
            .unwrap()
            .unwrap();
        assert_eq!(actual_block_body, block_bodies[1]);

        let actual_mock_transfer: BlockHash = txn
            .get(
                *destination_fixture
                    .db(Some(TransferDatabase::db_name()))
                    .unwrap(),
                &block_hash_1,
            )
            .map(bincode::deserialize)
            .unwrap()
            .unwrap();
        assert_eq!(block_hash_1, actual_mock_transfer);

        for deploy_hash in actual_block_body.deploy_hashes() {
            let actual_mock_deploy: DeployHash = txn
                .get(
                    *destination_fixture
                        .db(Some(DeployDatabase::db_name()))
                        .unwrap(),
                    deploy_hash,
                )
                .map(bincode::deserialize)
                .unwrap()
                .unwrap();
            assert_eq!(*deploy_hash, actual_mock_deploy);

            let mut actual_deploy_metadata: DeployMetadata = txn
                .get(
                    *destination_fixture
                        .db(Some(DeployMetadataDatabase::db_name()))
                        .unwrap(),
                    deploy_hash,
                )
                .map(bincode::deserialize)
                .unwrap()
                .unwrap();
            assert!(actual_deploy_metadata
                .execution_results
                .remove(&block_hash_1)
                .is_some());
            assert!(actual_deploy_metadata.execution_results.is_empty());
        }

        assert_eq!(
            txn.get(
                *destination_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[2].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
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
    let source_env = LmdbEnvironment::new(source_tmp_dir.path(), max_db_size, 512, true).unwrap();
    let source_store = LmdbTrieStore::new(&source_env, None, DatabaseFlags::empty()).unwrap();
    // Construct mock data.
    let data = create_data();

    {
        // Put the generated data into the source trie.
        let mut txn = source_env.create_read_write_txn().unwrap();
        let items = data.iter().map(Into::into);
        source_store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    let (_source_state, _env) =
        load_execution_engine(source_tmp_dir.path(), max_db_size, Digest::default(), true).unwrap();

    let (_destination_state, dst_env) =
        create_execution_engine(destination_tmp_dir.path(), max_db_size, true).unwrap();

    // Copy from `node2`, the root of the created trie. All data under node 2,
    // which has leaf 2 and 3 under it, should be copied.
    global_state::transfer_global_state(
        source_tmp_dir.path(),
        destination_tmp_dir.path(),
        data[4].0,
    )
    .unwrap();

    let destination_store = LmdbTrieStore::new(&dst_env, None, DatabaseFlags::empty()).unwrap();
    {
        let txn = dst_env.create_read_write_txn().unwrap();
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
                        Digest::hash(trie.to_bytes().unwrap())
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
