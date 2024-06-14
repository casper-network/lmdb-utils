use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, Transaction};

use super::{Database, DeserializationError};

pub struct TransactionsDatabase;

impl Display for TransactionsDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "transactions")
    }
}

impl Database for TransactionsDatabase {
    fn db_name() -> &'static str {
        "transactions"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Transaction = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
