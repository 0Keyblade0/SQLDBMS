use crate::common::Result;
use crate::sql::engine::Transaction;
use crate::sql::planner::Expression;
use crate::storage::page::INVALID_RID;
use crate::storage::tuple::{Row, Rows};
use crate::types::field::Field;
use crate::types::Table;

/// A table source via sequential scan
pub fn scan(txn: &impl Transaction, table: Table, filter: Option<Expression>) -> Result<Rows> {
    txn.scan(table.name(), filter)
}

/// Returns nothing. Used to short-circuit nodes that can't produce any rows.
pub fn nothing() -> Rows {
    Box::new(std::iter::empty())
}

/// Emits predefined constant values.
pub fn values(tuples: Vec<Vec<Expression>>) -> Rows {
    let iter = tuples.into_iter().map(|tuple| {
        let evaluated: Result<Vec<Field>> =
            tuple.into_iter().map(|expr| expr.evaluate(None)).collect();
        evaluated.map(|fields| (INVALID_RID, Row::from(fields)))
    });
    Box::new(iter)
}
