use std::collections::BTreeMap;
use itertools::Itertools;
use crate::common::Result;
use crate::sql::engine::Transaction;
use crate::sql::planner::Expression;
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::Table;

/// Deletes rows, taking primary keys from the source (i.e. DELETE) using the
/// primary_key column index. Returns the number of rows deleted.
pub fn delete(txn: &impl Transaction, table: String, source: Rows) -> Result<u64> {
    let mut rows = Vec::new();
    for row in source {
        rows.push(row?.0);
    }
    txn.delete(&table, &rows)?;
    Ok(rows.len() as u64)
}

/// Inserts rows into a table (i.e. INSERT) from the given source.
/// Returns the record IDs corresponding to the rows inserted into the table.
pub fn insert(txn: &impl Transaction, table: Table, source: Rows) -> Result<Vec<RecordId>> {
    let mut rows = Vec::new();
    for row in source {
        rows.push(row?.1);
    }
    txn.insert(table.name(), rows)
}

/// Updates rows passed in from the source (i.e. UPDATE). Returns the number of
/// rows updated.
///
/// Hint: `<T,E> Option<Result<T,E>>::transpose(self) -> Result<Option<T>, E>` and
/// the `?` operator might be useful here. An example of `transpose` from the docs:
/// ```
/// #[derive(Debug, Eq, PartialEq)]
/// struct SomeErr;
///
/// let x: Result<Option<i32>, SomeErr> = Ok(Some(5));
/// let y: Option<Result<i32, SomeErr>> = Some(Ok(5));
/// assert_eq!(x, y.transpose());
/// ```
pub fn update(
    txn: &impl Transaction,
    table: String,
    mut source: Rows,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {

    let mut x = BTreeMap::new();

    for row in source {
        let mut new_row = row.clone()?.1;
        let new_row1 = row.clone()?.1;
        for exp in expressions.clone() {
            new_row.update_field(exp.0,exp.1.evaluate(Some(&new_row1))?)?;
        }
        x.insert(row.clone()?.0, new_row);
    }

    txn.update(&table, x.clone())?;
    Ok(x.len() as u64)

}
