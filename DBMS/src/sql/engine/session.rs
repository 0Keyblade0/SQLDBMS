use super::Engine;
use crate::common::{Error, Result};
use crate::sql::execution::ExecutionResult;
use crate::sql::parser::Parser;
use crate::sql::planner::Plan;
use crate::storage::page::RecordId;
use crate::storage::tuple::Row;
use crate::types::field::Label;
use serde::{Deserialize, Serialize};

/// A SQL session, which executes raw SQL statements against a query engine.
pub struct Session<'a, E: Engine<'a>> {
    txn: E::Transaction,
}

impl<'a, E: Engine<'a>> Session<'a, E> {
    /// Creates a new session with the given query engine.
    pub fn new(engine: &'a E) -> Self {
        Self {
            txn: engine.begin().expect("Could not begin new transaction."),
        }
    }

    /// Executes a raw SQL statement.
    pub fn execute(&mut self, statement: &str) -> Result<StatementResult> {
        Plan::build(Parser::new(statement).parse()?, &self.txn)?
            .optimize()?
            .execute(&self.txn)?
            .try_into()
    }
}

/// A session statement result. Sent across the wire to SQL clients.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum StatementResult {
    Explain(Plan),
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
        columns: Vec<Label>,
        rows: Vec<Row>,
    },
}

/// Converts an execution result into a statement result.
impl TryFrom<ExecutionResult> for StatementResult {
    type Error = Error;
    fn try_from(result: ExecutionResult) -> Result<Self> {
        Ok(match result {
            ExecutionResult::CreateTable { name } => Self::CreateTable { name },
            ExecutionResult::DropTable { name, existed } => Self::DropTable { name, existed },
            ExecutionResult::Delete { count } => Self::Delete { count },
            ExecutionResult::Insert { count, record_ids } => Self::Insert { count, record_ids },
            ExecutionResult::Update { count } => Self::Update { count },
            ExecutionResult::Select { rows, columns } => {
                let rows: Result<Vec<_>> = rows.into_iter().map(|r| Ok(r?.1)).collect();
                Self::Select {
                    columns,
                    rows: rows?,
                }
            }
        })
    }
}
