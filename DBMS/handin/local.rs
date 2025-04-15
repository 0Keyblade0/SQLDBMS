use crate::common::Result;
use crate::sql::engine::{Catalog, Session};
use crate::sql::planner::Expression;
use crate::storage::page::RecordId;
use crate::storage::simple::Simple;
use crate::storage::tuple::{Row, Rows};
use crate::storage::{simple, Key};
use crate::types::field::Field;
use crate::types::Table;
use crate::{errinput, storage};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use crate::common::Error::InvalidInput;

/// A SQL engine using local storage. This is a single-transaction,
/// basic execution engine without concurrency support.
pub struct Local<E: storage::Engine + 'static> {
    /// The local non-concurrent storage engine.
    pub simple: Simple<E>,
}

impl<'a, E: storage::Engine> Local<E> {
    /// Creates a new local SQL engine using the given storage engine.
    pub fn new(engine: E) -> Self {
        Self {
            simple: Simple::new(engine),
        }
    }

    /// Creates a session which executes SQL statements.
    /// Does not outlive engine.
    pub fn session(&'a self) -> Session<'a, Self> {
        Session::new(self)
    }
}

impl<'a, E: storage::Engine> super::Engine<'a> for Local<E> {
    type Transaction = Transaction<E>;

    fn begin(&'a self) -> Result<Self::Transaction> {
        Ok(Transaction::new(self.simple.begin()?))
    }
}

/// A SQL transaction, wrapping a simple transaction.
pub struct Transaction<E: storage::Engine + 'static> {
    txn: simple::Transaction<E>,
}

#[allow(dead_code)]
impl<E: storage::Engine> Transaction<E> {
    /// Creates a new SQL transaction using the given simple transaction.
    /// This "transaction" is just a reference to the engine wrapped in a mutex.
    fn new(txn: simple::Transaction<E>) -> Self {
        Self { txn }
    }
}

/// See `[super::Transaction]` for method documentation.
impl<E: storage::Engine> super::Transaction for Transaction<E> {
    fn delete(&self, table_name: &str, ids: &[RecordId]) -> Result<()> {
        for rid in ids.iter() {
            self.txn.delete(Key::new(table_name, rid))?;
        }
        Ok(())
    }

    fn insert(&self, table_name: &str, rows: Vec<Row>) -> Result<Vec<RecordId>> {
        let schema = self.txn.fetch_table(table_name)?.unwrap();
        rows.into_iter()
            .map(|row| self.txn.insert(table_name, row.to_tuple(&schema)?))
            .collect()
    }

    fn scan(&self, table_name: &str, filter: Option<Expression>) -> Result<Rows> {
        let schema = self.txn.fetch_table(table_name)?.unwrap();
        let unpack = move |(rid, tuple)| (rid, Row::from_tuple(tuple, &schema).unwrap());
        let iter = self.txn.scan(table_name);

        // No filter; just return a row iterator
        let Some(filter) = filter else {
            return Ok(Box::new(
                iter.map(move |result| result.and_then(|item| Ok(unpack(item)))),
            ));
        };
        // Return a row iterator that filters out tuples that do not satisfy the predicate.
        let iter = iter.filter_map(move |result| {
            result
                .and_then(|item| {
                    let (rid, row) = unpack(item);
                    match filter.evaluate(Some(&row))? {
                        Field::Boolean(true) => Ok(Some((rid, row))),
                        Field::Boolean(false) | Field::Null => Ok(None),
                        value => errinput!("filter returned {value}, expected boolean."),
                    }
                })
                .transpose()
        });
        Ok(Box::new(iter))
    }

    fn update(&self, table_name: &str, rows: BTreeMap<RecordId, Row>) -> Result<()> {
        let schema = self.must_get_table(table_name)?;
        for (rid, row) in rows {
            self.txn
                .update(Key::new(table_name, &rid), row.to_tuple(&schema)?)?;
        }
        Ok(())
    }
}

/// See `[crate::storage::Catalog]` for method documentation.
///
/// Hint: `self.txn` has helpful methods *cough* *cough* that you should use,
/// e.g. Transaction::create_table(). You also might need `Error::InvalidInput`.
impl<E: storage::Engine> Catalog for Transaction<E> {
    fn create_table(&self, table: Table) -> Result<()> {
        self.txn.create_table(table)
    }

    fn drop_table(&self, table_name: &str, if_exists: bool) -> Result<bool> {

        if self.txn.fetch_table(table_name)?.is_none() && !if_exists {
            panic!("Invalid Input")
        } else {
            self.txn.delete_table(table_name)
        }
    }

    fn get_table(&self, table_name: &str) -> Result<Option<Table>> {
        self.txn.fetch_table(table_name)
    }
}
