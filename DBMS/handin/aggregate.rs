use crate::common::Result;
use crate::sql::planner::{Aggregate, Expression};

use crate::storage::page::INVALID_RID;
use crate::storage::tuple::{Row, Rows};
use crate::types::field::Field;
use itertools::Itertools as _;
use std::collections::BTreeMap;

/// Aggregates row values from the source according to the aggregates, using the
/// group_by expressions as buckets. Emits rows with group_by buckets then
/// aggregates in the given order.
pub fn aggregate(
    mut source: Rows,
    group_by: Vec<Expression>,
    aggregates: Vec<Aggregate>,
) -> Result<Rows> {
    let mut aggregator = Aggregator::new(group_by, aggregates);
    while let Some((_, row)) = source.next().transpose()? {
        aggregator.add(row)?;
    }
    aggregator.into_rows()
}

/// Computes bucketed aggregates for rows.
struct Aggregator {
    /// Bucketed accumulators (by group_by values).
    ///
    /// For example, if we are computing COUNT and MAX aggregations over "GROUP BY id"
    /// and "GROUP BY name, age, height", then `buckets` would have two entries:
    /// - vec![ id ]                 -> vec![ Accumulator::Count, Accumulator::Max ]
    /// - vec![ name, age, height ]  -> vec![ Accumulator::Count, Accumulator::Max ]
    buckets: BTreeMap<Vec<Field>, Vec<Accumulator>>,
    /// The set of empty accumulators. Used to create new buckets.
    empty: Vec<Accumulator>,
    /// Group by expressions. Indexes map to bucket values.
    group_by: Vec<Expression>,
    /// Expressions to accumulate. Indexes map to accumulators.
    expressions: Vec<Expression>,
}

impl Aggregator {
    /// Creates a new aggregator for the given GROUP BY buckets and aggregates.
    fn new(group_by: Vec<Expression>, aggregates: Vec<Aggregate>) -> Self {
        use Aggregate::*;
        let accumulators = aggregates.iter().map(Accumulator::new).collect();
        let expressions = aggregates
            .into_iter()
            .map(|aggregate| match aggregate {
                Average(expr) | Count(expr) | Max(expr) | Min(expr) | Sum(expr) => expr,
            })
            .collect();
        Self {
            buckets: BTreeMap::new(),
            empty: accumulators,
            group_by,
            expressions,
        }
    }

    /// Adds a row to the aggregator.
    fn add(&mut self, row: Row) -> Result<()> {
        // Compute the bucket value.
        let bucket: Vec<Field> = self
            .group_by
            .iter()
            .map(|expr| expr.evaluate(Some(&row)))
            .try_collect()?;

        // Compute and accumulate the input values.
        //
        // You'll need to retrieve the entry for the given bucket from `self.buckets`
        // or initialize an empty accumulator if an entry doesn't exist. Then, you'll
        // have to update each accumulator with the result of evaluating the accumulator's
        // corresponding expression on the row.
        let accumulators = self.buckets.entry(bucket).or_insert_with(|| self.empty.clone());

        for (expr, accumulator) in self.expressions.iter().zip(accumulators.iter_mut()) {
            let value = expr.evaluate(Some(&row))?;
            accumulator.add(value)?;
        }

        Ok(())
    }

    /// Returns a row iterator over the aggregate result.
    fn into_rows(self) -> Result<Rows> {
        // If there were no rows and no group_by expressions, return a row of
        // empty accumulators, e.g. SELECT COUNT(*) FROM t WHERE FALSE
        if self.buckets.is_empty() && self.group_by.is_empty() {
            let result = Row::from(
                self.empty
                    .into_iter()
                    .map(|acc| acc.value())
                    .collect::<Result<Vec<_>>>()?,
            );
            return Ok(Box::new(std::iter::once(Ok((INVALID_RID, result)))));
        }

        // Emit the group_by and aggregate values for each bucket. We use an
        // intermediate vec since btree_map::IntoIter doesn't implement Clone
        // (required by Rows).
        let buckets = self.buckets.into_iter().collect_vec();
        Ok(Box::new(buckets.into_iter().map(
            |(bucket, accumulators)| {
                Ok((
                    INVALID_RID,
                    Row::from(
                        bucket
                            .into_iter()
                            .map(Ok)
                            .chain(accumulators.into_iter().map(|acc| acc.value()))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ))
            },
        )))
    }
}

/// Accumulates aggregate values. Uses an enum rather than a trait since we need
/// to keep these in a vector (could use boxed trait objects too).
#[derive(Clone)]
enum Accumulator {
    Average { count: i32, sum: Field },
    Count(i32),
    Max(Option<Field>),
    Min(Option<Field>),
    Sum(Option<Field>),
}

impl Accumulator {
    /// Creates a new accumulator from an aggregate kind.
    fn new(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Average(_) => Self::Average {
                count: 0,
                sum: Field::Integer(0),
            },
            Aggregate::Count(_) => Self::Count(0),
            Aggregate::Max(_) => Self::Max(None),
            Aggregate::Min(_) => Self::Min(None),
            Aggregate::Sum(_) => Self::Sum(None)
        }
    }

    /// Adds a value to the accumulator.
    ///
    /// Hint: The `@` syntax in patterns allows for the creation of a binding while
    /// also performing a pattern match. For example, if `self` is a `Self::Sum`
    /// accumulator that was just initialized (i.e. `add` hasn't been called on it yet),
    /// then `self` is `Self::Sum(None)`. However, in order to add (i.e. accumulate!) the
    /// input value into `self`'s running total, we'd need `self` to be `Self::Sum(Some(0))`.
    /// We can work around this ergonomic mismatch--which arises when pattern matching which
    /// variant of `Accumulator` that `self` is--with the `@` keyword as follows:
    ///
    /// ```rust
    ///  use rustydb::common::Result;
    ///  use rustydb::sql::planner::Node::Aggregate;
    ///  use rustydb::types::field::Field;
    ///
    ///  enum Accumulator {
    ///     Average { count: i32, sum: Field },
    ///     Count(i32),
    ///     Max(Option<Field>),
    ///     Min(Option<Field>),
    ///     Sum(Option<Field>),
    ///  }
    ///
    ///  fn add(acc: &mut Accumulator, value: Field) -> Result<()> {
    ///     // ...
    ///     match acc {
    ///         // Running accumulator value already exists; just add `value` to it!
    ///         Accumulator::Sum(Some(sum)) => *sum = sum.checked_add(&value)?,
    ///         // Running accumulator value does not exist; need to replace the
    ///         // `None` value of `acc` with Some(value).
    ///         Accumulator::Sum(sum @ None) => *sum = Some(Field::Integer(0).checked_add(&value)?),
    ///         // ...
    ///         _ => todo!()
    ///     }
    ///     // ...
    ///     todo!()
    ///  }
    /// ```
    fn add(&mut self, value: Field) -> Result<()> {

        match self {
            // accumulator value already exists: add value to acc
            Accumulator::Sum(Some(sum)) => *sum = sum.checked_add(&value)?,

            // accumulator value does not exist: assign value of acc with Some()
            Accumulator::Sum(sum @ None) => *sum = Some(Field::Integer(0).checked_add(&value)?),

            // accumulator value already exists: increment the count if value is not null
            Accumulator::Count(count) => {

                if value != Field::Null {
                    *count += 1;
                }
            },

            // accumulator value already exists: update max if needed
            Accumulator::Max(Some(existing_max)) => {
                if value > *existing_max {
                    *existing_max = value; // Update max value if current value is larger
                }
            },

            // accumulator value does not exist: initialize max with value
            Accumulator::Max(max @ None) => *max = Some(value), // Initialize max with the first value

            // accumulator value already exists: update min if needed
            Accumulator::Min(Some(existing_min)) => {
                if value < *existing_min {
                    *existing_min = value; // Update min value if current value is smaller
                }
            },

            // accumulator value does not exist: initialize min with value
            Accumulator::Min(min @ None) => *min = Some(value), // Initialize min with the first value

            // increment the count and add to the sum
            Accumulator::Average { count, sum } => {
                *count += 1; // Increment count
                *sum = sum.checked_add(&value)?; // Add value to sum
            }
        }

        Ok(())
    }

    /// Returns the aggregate value.
    fn value(self) -> Result<Field> {
        match self {
            // Count
            Accumulator::Count(count) => Ok(Field::Integer(count)),

            // Sum: return the sum if it exists, else return a default value of 0
            Accumulator::Sum(Some(sum)) => Ok(sum),
            Accumulator::Sum(None) => Ok(Field::Null),

            // Max: return the max value if it exists, else return NULL.
            Accumulator::Max(Some(max)) => Ok(max),
            Accumulator::Max(None) => Ok(Field::Null),

            // Min: return the min value if it exists, else return NULL.
            Accumulator::Min(Some(min)) => Ok(min),
            Accumulator::Min(None) => Ok(Field::Null),

            // Average: calculate the average if there is at least one value, else return NULL.
            Accumulator::Average { count, sum } => {
                if count > 0 {
                    // Safely divide the sum by the count to calculate the average.
                    sum.checked_div(&Field::Integer(count))
                } else {
                    Ok(Field::Null) // No values to average, return NULL.
                }
            }
        }
    }
}
