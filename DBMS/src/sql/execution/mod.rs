//! SQL Query Execution Engine and related machinery.
mod aggregate;
mod execute;
mod join;
mod source;
mod transform;
mod write;

pub use execute::{execute_plan, ExecutionResult};
