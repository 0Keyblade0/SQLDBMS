use crate::common::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution;
use crate::sql::execution::ExecutionResult;
use crate::sql::parser::ast;
use crate::sql::planner::expression::Expression;
use crate::sql::planner::optimizer::OPTIMIZERS;
use crate::sql::planner::{BoxedNode, Node, Planner};
use crate::types::Table;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    /// A CREATE TABLE plan. Creates a new table with the given schema. Errors
    /// if the table already exists or the schema is invalid.
    CreateTable { schema: Table },
    /// A DROP TABLE plan. Drops the given table. Errors if the table does not
    /// exist, unless if_exists is true.
    DropTable { table: String, if_exists: bool },
    /// A DELETE plan. Deletes rows in table that match the rows from source.
    /// primary_key specifies the primary key column index in the source rows.
    Delete {
        table: String,
        // primary_key: usize,
        source: BoxedNode,
    },
    /// An INSERT plan. Inserts rows from source (typically a Values node) into table.
    Insert { table: Table, source: BoxedNode },
    /// An UPDATE plan. Updates rows in table that match the rows from source,
    /// where primary_key specifies the primary key column index in the source
    /// rows. The given column/expression pairs specify the row updates to make,
    /// evaluated using the existing source row, which must be a complete row
    /// from the update table.
    Update {
        table: Table,
        // primary_key: usize,
        source: BoxedNode,
        expressions: Vec<(usize, Expression)>,
    },
    /// A SELECT plan. Recursively executes the query plan tree and returns the
    /// resulting rows.
    Select(BoxedNode),
}

impl Plan {
    /// Builds a plan from an AST statement.
    pub fn build(statement: ast::Statement, catalog: &impl Catalog) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// Executes the plan, consuming it.
    pub fn execute(self, txn: &(impl Transaction + Catalog)) -> Result<ExecutionResult> {
        execution::execute_plan(self, txn, txn)
    }

    /// Optimizes the plan, consuming it.
    pub fn optimize(self) -> Result<Self> {
        let optimize = |node| OPTIMIZERS.iter().try_fold(node, |node, (_, opt)| opt(node));
        Ok(match self {
            Self::CreateTable { .. } | Self::DropTable { .. } => self,
            Self::Delete { table, source } => Self::Delete {
                table,
                source: optimize(source)?,
            },
            Self::Insert { table, source } => Self::Insert {
                table,
                source: optimize(source)?,
            },
            Self::Update {
                table,
                source,
                expressions,
            } => Self::Update {
                table,
                source: optimize(source)?,
                expressions,
            },
            Self::Select(root) => Self::Select(optimize(root)?),
        })
    }
}

/// An aggregate function.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Average(Expression),
    Count(Expression),
    Max(Expression),
    Min(Expression),
    Sum(Expression),
}

#[allow(dead_code)]
impl Aggregate {
    fn format(&self, node: &Node) -> String {
        match self {
            Self::Average(expr) => format!("avg({})", expr.format(node)),
            Self::Count(expr) => format!("count({})", expr.format(node)),
            Self::Max(expr) => format!("max({})", expr.format(node)),
            Self::Min(expr) => format!("min({})", expr.format(node)),
            Self::Sum(expr) => format!("sum({})", expr.format(node)),
        }
    }
}

/// A sort order direction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ascending => f.write_str("asc"),
            Self::Descending => f.write_str("desc"),
        }
    }
}

impl From<ast::Direction> for Direction {
    fn from(dir: ast::Direction) -> Self {
        match dir {
            ast::Direction::Ascending => Self::Ascending,
            ast::Direction::Descending => Self::Descending,
        }
    }
}

/// Inverts a Remap targets vector to a vector of source indexes, with None
/// for columns that weren't targeted.
pub fn remap_sources(targets: &[Option<usize>]) -> Vec<Option<usize>> {
    let size = targets
        .iter()
        .filter_map(|v| *v)
        .map(|i| i + 1)
        .max()
        .unwrap_or(0);
    let mut sources = vec![None; size];
    for (from, to) in targets.iter().enumerate() {
        if let Some(to) = to {
            sources[*to] = Some(from);
        }
    }
    sources
}
