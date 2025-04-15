use crate::common::Result;
use crate::sql::planner::BoxedNode;
//
// /// A plan optimizer, which recursively transforms a plan node to make plan
// /// execution more efficient where possible.
pub type Optimizer = fn(BoxedNode) -> Result<BoxedNode>;
//
// /// The set of optimizers, and the order in which they are applied.
pub static OPTIMIZERS: &[(&str, Optimizer)] = &[];
