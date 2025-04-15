use crate::common::Result;
use crate::sql::planner::{Aggregate, Direction, Expression};
use crate::types::field::{Field, Label};
use crate::types::Table;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// A wrapper object holding a query plan node.
///
/// TODO: Stores schema information and rudimentary statistics for query
///       optimization tasks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoxedNode {
    pub(crate) inner: Box<Node>,
}

impl From<Node> for BoxedNode {
    fn from(node: Node) -> Self {
        Self {
            inner: Box::new(node),
        }
    }
}

impl Deref for BoxedNode {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

/// A query plan node. Returns a row iterator, and can be nested.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    /// Computes the given aggregate values for the given group_by buckets
    /// across all rows in the source node. The group_by columns are emitted
    /// first, followed by the aggregate columns, in the given order.
    Aggregate {
        source: BoxedNode,
        group_by: Vec<Expression>,
        aggregates: Vec<Aggregate>,
    },
    /// Filters source rows, by discarding rows for which the predicate
    /// evaluates to false.
    Filter {
        source: BoxedNode,
        predicate: Expression,
    },
    /// Joins the left and right sources on the given columns by building an
    /// in-memory hashmap of the right source and looking up matches for each
    /// row in the left source. When outer is true (e.g. LEFT JOIN), a left row
    /// without a right match is emitted anyway, with NULLs for the right row.
    HashJoin {
        left: BoxedNode,
        left_column: usize,
        right: BoxedNode,
        right_column: usize,
        outer: bool,
    },
    /// Looks up the given values in a secondary index and emits matching rows.
    /// NULL and NaN values are considered equal, to allow IS NULL and IS NAN
    /// index lookups, as is -0.0 and 0.0.
    IndexLookup {
        table: Table,
        column: usize,
        values: Vec<Field>,
        alias: Option<String>,
    },
    /// Looks up the given primary keys and emits their rows.
    KeyLookup {
        table: Table,
        keys: Vec<Field>,
        alias: Option<String>,
    },
    /// Only emits the first limit rows from the source, discards the rest.
    Limit { source: BoxedNode, limit: usize },
    /// Joins the left and right sources on the given predicate by buffering the
    /// right source and iterating over it for every row in the left source.
    /// When outer is true (e.g. LEFT JOIN), a left row without a right match is
    /// emitted anyway, with NULLs for the right row.
    NestedLoopJoin {
        left: BoxedNode,
        right: BoxedNode,
        predicate: Option<Expression>,
        outer: bool,
    },
    /// Nothing does not emit anything, and is used to short-circuit nodes that
    /// can't emit anything during optimization. It retains the column names of
    /// any replaced nodes for results headers and plan formatting.
    Nothing { columns: Vec<Label> },
    /// Discards the first offset rows from source, emits the rest.
    Offset { source: BoxedNode, offset: usize },
    /// Sorts the source rows by the given sort key. Buffers the entire row set
    /// in memory.
    Order {
        source: BoxedNode,
        key: Vec<(Expression, Direction)>,
    },
    /// Projects the input rows by evaluating the given expressions. Aliases are
    /// only used when displaying the plan.
    Projection {
        source: BoxedNode,
        expressions: Vec<Expression>,
        aliases: Vec<Label>,
    },
    /// Remaps source columns to the given target column index, or None to drop
    /// the column. Unspecified target columns yield Value::Null. The source →
    /// target mapping ensures a source column can only be mapped to a single
    /// target column, allowing the value to be moved rather than cloned.
    Remap {
        source: BoxedNode,
        targets: Vec<Option<usize>>,
    },
    /// A full table scan, with an optional pushed-down filter. The schema is
    /// used during plan optimization. The alias is only used for formatting.
    Scan {
        table: Table,
        filter: Option<Expression>,
        alias: Option<String>,
    },
    /// A constant set of values.
    Values { rows: Vec<Vec<Expression>> },
}

impl Node {
    /// Returns the number of columns emitted by the node.
    pub fn columns(&self) -> usize {
        match self {
            // Source nodes emit all table columns.
            Self::IndexLookup { table, .. }
            | Self::KeyLookup { table, .. }
            | Self::Scan { table, .. } => table.col_count(),

            // Some nodes modify the column set.
            Self::Aggregate {
                aggregates,
                group_by,
                ..
            } => aggregates.len() + group_by.len(),
            Self::Projection { expressions, .. } => expressions.len(),
            Self::Remap { targets, .. } => targets
                .iter()
                .filter_map(|v| *v)
                .map(|i| i + 1)
                .max()
                .unwrap_or(0),

            // Join nodes emit the combined columns.
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                left.columns() + right.columns()
            }

            // Simple nodes just pass through the source columns.
            Self::Filter { source, .. }
            | Self::Limit { source, .. }
            | Self::Offset { source, .. }
            | Self::Order { source, .. } => source.columns(),

            // And some are trivial.
            Self::Nothing { columns } => columns.len(),
            Self::Values { rows } => rows.first().map(|row| row.len()).unwrap_or(0),
        }
    }

    /// Returns a label for a column, if any, by tracing the column through the
    /// plan tree. Only used for query result headers and plan display purposes,
    /// not to look up expression columns (see Scope).
    #[allow(dead_code)]
    pub fn column_label(&self, index: usize) -> Label {
        match self {
            // Source nodes use the table/column name.
            Self::IndexLookup {
                table, alias: _, ..
            }
            | Self::KeyLookup {
                table, alias: _, ..
            }
            | Self::Scan {
                table, alias: _, ..
            } => Label::Qualified(
                table.name().parse().unwrap(),
                table.get_column(index).get_name(),
            ),

            // Some nodes rearrange columns. Route them to the correct
            // upstream column where appropriate.
            Self::Aggregate {
                source, group_by, ..
            } => match group_by.get(index) {
                Some(Expression::Column(index)) => source.column_label(*index),
                Some(_) | None => Label::None,
            },
            Self::Projection {
                source,
                expressions,
                aliases,
            } => match aliases.get(index) {
                Some(Label::None) | None => match expressions.get(index) {
                    // Unaliased column references route to the source.
                    Some(Expression::Column(index)) => source.column_label(*index),
                    // Unaliased expressions don't have a name.
                    Some(_) | None => Label::None,
                },
                // Aliased columns use the alias.
                Some(alias) => alias.clone(),
            },
            Self::Remap { source, targets } => targets
                .iter()
                .position(|t| t == &Some(index))
                .map(|i| source.column_label(i))
                .unwrap_or(Label::None),

            // Joins dispatch to the appropriate source.
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                if index < left.columns() {
                    left.column_label(index)
                } else {
                    right.column_label(index - left.columns())
                }
            }

            // Simple nodes just dispatch to the source.
            Self::Filter { source, .. }
            | Self::Limit { source, .. }
            | Self::Offset { source, .. }
            | Self::Order { source, .. } => source.column_label(index),

            // Nothing nodes contain the original columns of replaced nodes.
            Self::Nothing { columns } => columns.get(index).cloned().unwrap_or(Label::None),

            // And some don't have any names at all.
            Self::Values { .. } => Label::None,
        }
    }

    /// Recursively transforms query nodes depth-first by applying the given
    /// closures before and after descending.
    pub fn transform(
        mut self,
        before: &impl Fn(Self) -> Result<Self>,
        after: &impl Fn(Self) -> Result<Self>,
    ) -> Result<Self> {
        // Helper for transforming boxed nodes.
        let xform = |mut node: BoxedNode| -> Result<BoxedNode> {
            *node.inner = node.inner.transform(before, after)?;
            Ok(node)
        };

        self = before(self)?;
        self = match self {
            Self::Aggregate {
                source,
                group_by,
                aggregates,
            } => Self::Aggregate {
                source: xform(source)?,
                group_by,
                aggregates,
            },
            Self::Filter { source, predicate } => Self::Filter {
                source: xform(source)?,
                predicate,
            },
            Self::HashJoin {
                left,
                left_column,
                right,
                right_column,
                outer,
            } => Self::HashJoin {
                left: xform(left)?,
                left_column,
                right: xform(right)?,
                right_column,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit {
                source: xform(source)?,
                limit,
            },
            Self::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => Self::NestedLoopJoin {
                left: xform(left)?,
                right: xform(right)?,
                predicate,
                outer,
            },
            Self::Offset { source, offset } => Self::Offset {
                source: xform(source)?,
                offset,
            },
            Self::Order { source, key } => Self::Order {
                source: xform(source)?,
                key,
            },
            Self::Projection {
                source,
                expressions,
                aliases,
            } => Self::Projection {
                source: xform(source)?,
                expressions,
                aliases,
            },
            Self::Remap { source, targets } => Self::Remap {
                source: xform(source)?,
                targets,
            },

            Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Nothing { .. }
            | Self::Scan { .. }
            | Self::Values { .. } => self,
        };
        self = after(self)?;
        Ok(self)
    }

    /// Recursively transforms all node expressions by calling the given
    /// closures on them before and after descending.
    pub fn transform_expressions(
        self,
        before: &impl Fn(Expression) -> Result<Expression>,
        after: &impl Fn(Expression) -> Result<Expression>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Filter {
                source,
                mut predicate,
            } => {
                predicate = predicate.transform(before, after)?;
                Self::Filter { source, predicate }
            }
            Self::NestedLoopJoin {
                left,
                right,
                predicate: Some(predicate),
                outer,
            } => {
                let predicate = Some(predicate.transform(before, after)?);
                Self::NestedLoopJoin {
                    left,
                    right,
                    predicate,
                    outer,
                }
            }
            Self::Order { source, mut key } => {
                key = key
                    .into_iter()
                    .map(|(expr, dir)| Ok((expr.transform(before, after)?, dir)))
                    .collect::<Result<_>>()?;
                Self::Order { source, key }
            }
            Self::Projection {
                source,
                mut expressions,
                aliases,
            } => {
                expressions = expressions
                    .into_iter()
                    .map(|expr| expr.transform(before, after))
                    .collect::<Result<Vec<Expression>>>()?;
                // .try_collect()?;
                Self::Projection {
                    source,
                    expressions,
                    aliases,
                }
            }
            Self::Scan {
                table,
                alias,
                filter: Some(filter),
            } => {
                let filter = Some(filter.transform(before, after)?);
                Self::Scan {
                    table,
                    alias,
                    filter,
                }
            }
            Self::Values { mut rows } => {
                rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| expr.transform(before, after))
                            .collect()
                    })
                    .collect::<Result<Vec<Vec<Expression>>>>()?;
                // .try_collect()?;
                Self::Values { rows }
            }

            Self::Aggregate { .. }
            | Self::HashJoin { .. }
            | Self::IndexLookup { .. }
            | Self::KeyLookup { .. }
            | Self::Limit { .. }
            | Self::NestedLoopJoin {
                predicate: None, ..
            }
            | Self::Nothing { .. }
            | Self::Offset { .. }
            | Self::Remap { .. }
            | Self::Scan { filter: None, .. } => self,
        })
    }
}
