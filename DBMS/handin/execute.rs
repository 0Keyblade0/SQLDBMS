use crate::common::Result;
use crate::errinput;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{aggregate, join, source, transform, write};
use crate::sql::execution::source::scan;
use crate::sql::execution::transform::{filter, limit, offset, project};
use crate::sql::planner::{BoxedNode, Node, Plan};
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::field::Label;

/// Executes a query plan.
///
/// Takes both a catalog and transaction as parameters, even though a transaction
/// implements the Catalog trait, to separate the concerns of `catalog` to planning
/// and `txn` to execution.
///
/// Hint: `execute(source, txn)?` returns a `Rows` source iterator, which you might
/// need for some of the plans. (The `execute` method actually returns `Result<Rows>`,
/// but the `?` operator will automatically unwrap the result if it's an `Ok(Rows)`
/// value. Otherwise, the method will immediately exit and return the `Err()` value
/// returned from `execute`.) For more information about the try-operator `?`, see:
/// - https://doc.rust-lang.org/rust-by-example/std/result/question_mark.html
/// - https://stackoverflow.com/questions/42917566/what-is-this-question-mark-operator-about
pub fn execute_plan(
    plan: Plan,
    catalog: &impl Catalog,
    txn: &impl Transaction,
) -> Result<ExecutionResult> {
    Ok(match plan {
        // Creates a table with the given schema, returning a `CreateTable` execution
        // result if the table creation is successful.
        //
        // You'll need to handle the case when `Catalog::create_table` returns an Error
        // (hint: use the ? operator).
        Plan::CreateTable { schema } => {
            catalog.create_table(schema.clone())?;
            ExecutionResult::CreateTable {name: schema.name().to_string()}
        }
        // Deletes the rows emitted from the source node from the given table.
        //
        // Hint: you'll need to use the `write::delete` method that you also have implement,
        // which returns the number of rows that were deleted if successful (another hint:
        // use the ? operator. Last reminder!).
        Plan::Delete { table, source } => {
            ExecutionResult::Delete {count: write::delete(txn, table, execute(source, txn)?)?}
        }
        // Drops the given table.
        //
        // Returns an error if the table does not exist unless `if_exists` is true.
        Plan::DropTable { table, if_exists } => {
            ExecutionResult::DropTable {name: table.clone(), existed: catalog.drop_table(&table, if_exists)?}
        }
        // Inserts the rows emitted from the source node into the given table.
        //
        // Hint: you'll need to use the `write::insert` method that you have to implement,
        // which returns the record id's corresponding to the rows that were inserted into
        // the table.
        Plan::Insert { table, source } => {
            let insert_ids = write::insert(txn, table, execute(source, txn)?)?;
            ExecutionResult::Insert {count: insert_ids.len() as u64, record_ids: insert_ids}
        }
        // Obtains a `Rows` iterator of the emitted rows and the emitted rows' corresponding
        // column labels from the root node, packaging the two as an `ExecutionResult::Select`.
        //
        // Hint: the i'th column label of a row emitted from the root can be obtained by calling
        // `root.column_label(i)`.
        Plan::Select(root) => {
            let rows_from = execute(root.clone(), txn)?;
            let mut labels = Vec::new();
            for index in 0..root.columns() {
               labels.push(root.column_label(index));
            }
            ExecutionResult::Select {rows: rows_from , columns: labels}
        }
        // Updates the rows emitted from the source node in the given table.
        //
        // Hint: you'll have to use the `write::update` method that you have implement, which
        // returns the number of rows update if successful.
        Plan::Update {
            table,
            source,
            expressions,
        } => {
            ExecutionResult::Update {count: write::update(txn,
                                                          table.name().to_string(),
                                                          execute(source, txn)?,
                                                          expressions)?}
        }
    })
}

/// Recursively executes a query plan node, returning a tuple iterator.
///
/// Tuples stream through the plan node tree from the branches to the root. Nodes
/// recursively pull input rows upwards from their child node(s), process them,
/// and hand the resulting rows off to their parent node.
pub fn execute(node: BoxedNode, txn: &impl Transaction) -> Result<Rows> {
    Ok(match *node.inner {
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let source = execute(source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)?
        }

        Node::Filter { source, predicate } => {
            let source = execute(source, txn)?;
            filter(source, predicate)
        }

        Node::HashJoin {
            left,
            left_column,
            right,
            right_column,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::hash(left, left_column, right, right_column, right_size, outer)?
        }

        Node::IndexLookup {
            table: _table,
            column: _column,
            values: _values,
            alias: _,
        } => {
            let columns = _table.columns();
            return if _column >= columns.len() {
                Err(errinput!("Invalid column index"))
            } else {
               let column_name = columns[_column].get_name().clone();
                todo!()
            };
        }

        Node::KeyLookup {
            table: _table,
            keys: _keys,
            alias: _,
        } => {
            todo!();
        }

        Node::Limit { source, limit } => {
            let source = execute(source, txn)?;
            transform::limit(source, limit)

        }

        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::nested_loop(left, right, right_size, predicate, outer)?
        }

        Node::Nothing { .. } => source::nothing(),

        Node::Offset {
            source: _source,
            offset: _offset,
        } => {
            let source = execute(_source, txn)?;
            offset(source, _offset)
        }

        Node::Order {
            source,
            key: orders,
        } => {
            let source = execute(source, txn)?;
            transform::order(source, orders)?
        }

        Node::Projection {
            source,
            expressions,
            aliases: _,
        } => {
            let source = execute(source, txn)?;
            project(source, expressions)
        }

        Node::Remap { source, targets } => {
            let source = execute(source, txn)?;
            transform::remap(source, targets)
        }

        Node::Scan {
            table,
            filter,
            alias: _,
        } => {
            scan(txn, table, filter)?
        }

        Node::Values { rows } => source::values(rows),
    })
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable {
        name: String,
    },
    DropTable {
        name: String,
        existed: bool,
    },
    Delete {
        count: u64,
    },
    Insert {
        count: u64,
        record_ids: Vec<RecordId>,
    },
    Update {
        count: u64,
    },
    Select {
        rows: Rows,
        columns: Vec<Label>,
    },
}
