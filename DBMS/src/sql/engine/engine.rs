use crate::common::Result;
use crate::errinput;
use crate::sql::planner::Expression;
use crate::storage::page::RecordId;
use crate::storage::tuple::{Row, Rows};
use crate::types::Table;
use std::collections::BTreeMap;

/// A SQL query engine.
///
/// Executes create, read, update, delete (CRUD) operations against the data
/// in its underlying database.
pub trait Engine<'a>: Sized {
    /// The engine's transaction type. It provides transactional access
    /// to table rows and schemas. It does not outlive the engine.
    type Transaction: Transaction + Catalog + 'a;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> Result<Self::Transaction>;
}

/// A SQL transaction.
///
/// Tuples are passed around as serialized byte streams, which can be deserialized
/// into `Tuple` instances with their corresponding Table schema definition.
///
/// Currently, all query execution tasks occur in a singleton transaction instance.
/// TODO(eyoon): Provide transactional execution with snapshot isolation (MVCC)
pub trait Transaction {
    /// Deletes tuples of a table by record id (RID), if they exist.
    fn delete(&self, table: &str, ids: &[RecordId]) -> Result<()>;
    /// Inserts tuples into a table, and returns a vector of their corresponding record ids.
    fn insert(&self, table_name: &str, rows: Vec<Row>) -> Result<Vec<RecordId>>;
    /// Sequentially scans a table's tuples, applying a filter if specified.
    fn scan(&self, table_name: &str, filter: Option<Expression>) -> Result<Rows>;
    /// Updates the table's tuples with record id in `rows` to the corresponding given tuple.
    fn update(&self, table_name: &str, rows: BTreeMap<RecordId, Row>) -> Result<()>;
}

/// Stores table schema information.
pub trait Catalog {
    /// Creates a new table. Errors if the specified table already exists.
    fn create_table(&self, table: Table) -> Result<()>;
    /// Drops the table corresponding to `table_name`.
    /// If such a table exists and was dropped, returns `true`.
    /// Returns `false` otherwise.
    fn drop_table(&self, table_name: &str, if_exists: bool) -> Result<bool>;
    /// Fetches the schema for the table corresponding to `table_name`.
    /// Returns `None` if no such table exists.
    fn get_table(&self, table_name: &str) -> Result<Option<Table>>;

    /// Fetches the schema for the table corresponding to `table_id`.
    /// Errors if no such table exists.
    fn must_get_table(&self, table_name: &str) -> Result<Table> {
        self.get_table(table_name)?
            .ok_or_else(|| errinput!("No table with name {table_name} exists."))
    }
}
