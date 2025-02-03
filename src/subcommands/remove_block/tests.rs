use std::slice;

use casper_node::types::{BlockHash, DeployHash, DeployMetadata};
use lmdb::{Error as LmdbError, Transaction, WriteFlags};

use crate::{
    common::db::{
        BlockBodyDatabase, BlockHeaderDatabase, Database, DeployMetadataDatabase, STORAGE_FILE_NAME,
    },
    subcommands::{
        execution_results_summary::block_body::BlockBody,
        remove_block::{remove::remove_block, Error},
    },
    test_utils::{
        mock_block_header, mock_deploy_hash, mock_deploy_metadata, LmdbTestFixture, MockBlockHeader,
    },
};

#[test]
fn remove_block_should_work() {
    const BLOCK_COUNT: usize = 2;
    const DEPLOY_COUNT: usize = 3;

    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8).map(mock_deploy_hash).collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> =
        (0..BLOCK_COUNT as u8).map(mock_block_header).collect();
    let mut block_bodies = vec![];
    let mut block_body_deploy_map: Vec<Vec<usize>> = vec![];
    block_bodies.push(BlockBody::new(vec![deploy_hashes[0], deploy_hashes[1]]));
    block_body_deploy_map.push(vec![0, 1]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[1], deploy_hashes[2]]));
    block_body_deploy_map.push(vec![1, 2]);

    let deploy_metadatas = [
        mock_deploy_metadata(slice::from_ref(&block_headers[0].0)),
        mock_deploy_metadata(&[block_headers[0].0, block_headers[1].0]),
        mock_deploy_metadata(slice::from_ref(&block_headers[1].0)),
    ];

    // Insert the 2 blocks into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();
        for i in 0..BLOCK_COUNT {
            // Store the header.
            txn.put(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the body.
            txn.put(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[i].1.body_hash,
                &bincode::serialize(&block_bodies[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for i in 0..DEPLOY_COUNT {
            txn.put(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    assert!(remove_block(test_fixture.tmp_dir.path(), block_headers[0].0).is_ok());

    {
        let txn = test_fixture.env.begin_ro_txn().unwrap();
        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[0].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[1].0,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[0].1.body_hash,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[1].1.body_hash,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[0]
            )
            .unwrap_err(),
            LmdbError::NotFound
        );

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[1],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[2],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_no_deploys() {
    const BLOCK_COUNT: usize = 2;
    const DEPLOY_COUNT: usize = 3;

    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8).map(mock_deploy_hash).collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> =
        (0..BLOCK_COUNT as u8).map(mock_block_header).collect();
    let mut block_bodies = vec![];
    let mut block_body_deploy_map: Vec<Vec<usize>> = vec![];
    block_bodies.push(BlockBody::new(vec![]));
    block_body_deploy_map.push(vec![]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[1], deploy_hashes[2]]));
    block_body_deploy_map.push(vec![1, 2]);

    let deploy_metadatas = [
        mock_deploy_metadata(&[]),
        mock_deploy_metadata(slice::from_ref(&block_headers[1].0)),
        mock_deploy_metadata(slice::from_ref(&block_headers[1].0)),
    ];

    // Insert the 2 blocks into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();
        for i in 0..BLOCK_COUNT {
            // Store the header.
            txn.put(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the body.
            txn.put(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[i].1.body_hash,
                &bincode::serialize(&block_bodies[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the last 2 deploys into the deploys and deploy_metadata
        // databases.
        for i in 1..DEPLOY_COUNT {
            txn.put(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    assert!(remove_block(test_fixture.tmp_dir.path(), block_headers[0].0).is_ok());

    {
        let txn = test_fixture.env.begin_ro_txn().unwrap();
        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[0].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[1].0,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[0].1.body_hash,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[1].1.body_hash,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[0]
            )
            .unwrap_err(),
            LmdbError::NotFound
        );

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[1],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[2],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_missing_header() {
    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let (block_hash, _block_header) = mock_block_header(0);
    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::MissingHeader(actual_block_hash) if block_hash == actual_block_hash)
    );
}

#[test]
fn remove_block_missing_body() {
    const BLOCK_COUNT: usize = 2;
    const DEPLOY_COUNT: usize = 3;

    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8).map(mock_deploy_hash).collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> =
        (0..BLOCK_COUNT as u8).map(mock_block_header).collect();
    let mut block_bodies = vec![];
    let mut block_body_deploy_map: Vec<Vec<usize>> = vec![];
    block_bodies.push(BlockBody::new(vec![deploy_hashes[0], deploy_hashes[1]]));
    block_body_deploy_map.push(vec![0, 1]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[1], deploy_hashes[2]]));
    block_body_deploy_map.push(vec![1, 2]);

    let deploy_metadatas = [
        mock_deploy_metadata(slice::from_ref(&block_headers[0].0)),
        mock_deploy_metadata(&[block_headers[0].0, block_headers[1].0]),
        mock_deploy_metadata(slice::from_ref(&block_headers[1].0)),
    ];

    // Insert the 2 block headers into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            // Store the header.
            txn.put(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for i in 0..DEPLOY_COUNT {
            txn.put(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    assert!(remove_block(test_fixture.tmp_dir.path(), block_headers[0].0).is_ok());

    {
        let txn = test_fixture.env.begin_ro_txn().unwrap();
        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[0].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[1].0,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[0].1.body_hash,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert_eq!(
            txn.get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[1].1.body_hash,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_missing_deploys() {
    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let (block_hash, block_header) = mock_block_header(0);
    let deploy_hash = mock_deploy_hash(0);
    let block_body = BlockBody::new(vec![deploy_hash]);

    // Insert the block into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();

        // Store the header.
        txn.put(
            *test_fixture
                .db(Some(BlockHeaderDatabase::db_name()))
                .unwrap(),
            &block_hash,
            &bincode::serialize(&block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the body.
        txn.put(
            *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
            &block_header.body_hash,
            &bincode::serialize(&block_body).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    };

    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::MissingDeploy(actual_deploy_hash) if deploy_hash == actual_deploy_hash)
    );
}

#[test]
fn remove_block_invalid_header() {
    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let (block_hash, _block_header) = mock_block_header(0);

    // Insert the an invalid block header into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();

        // Store the header.
        txn.put(
            *test_fixture
                .db(Some(BlockHeaderDatabase::db_name()))
                .unwrap(),
            &block_hash,
            &[0u8, 1u8, 2u8],
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    };

    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::HeaderParsing(actual_block_hash, _) if block_hash == actual_block_hash)
    );
}

#[test]
fn remove_block_invalid_body() {
    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let (block_hash, block_header) = mock_block_header(0);

    // Insert the block header along with an invalid body into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();

        // Store the header.
        txn.put(
            *test_fixture
                .db(Some(BlockHeaderDatabase::db_name()))
                .unwrap(),
            &block_hash,
            &bincode::serialize(&block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the body.
        txn.put(
            *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
            &block_header.body_hash,
            &[0u8, 1u8, 2u8],
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    };

    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::BodyParsing(actual_block_hash, _) if block_hash == actual_block_hash)
    );
}

#[test]
fn remove_block_invalid_deploy_metadata() {
    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let (block_hash, block_header) = mock_block_header(0);
    let deploy_hash = mock_deploy_hash(0);
    let block_body = BlockBody::new(vec![deploy_hash]);

    // Insert the block into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();

        // Store the header.
        txn.put(
            *test_fixture
                .db(Some(BlockHeaderDatabase::db_name()))
                .unwrap(),
            &block_hash,
            &bincode::serialize(&block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the body.
        txn.put(
            *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
            &block_header.body_hash,
            &bincode::serialize(&block_body).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the deploy metadata.
        txn.put(
            *test_fixture
                .db(Some(DeployMetadataDatabase::db_name()))
                .unwrap(),
            &deploy_hash,
            &[0u8, 1u8, 2u8],
            WriteFlags::empty(),
        )
        .unwrap();

        txn.commit().unwrap();
    };

    assert!(
        matches!(remove_block(test_fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::ExecutionResultsParsing(actual_block_hash, actual_deploy_hash, _) if block_hash == actual_block_hash && deploy_hash == actual_deploy_hash)
    );
}
