use crate::sql::engine::{Local, Session, StatementResult};
use crate::storage::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::HeapTableManager;
use itertools::Itertools;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, Error, Read};
use std::sync::{Arc, RwLock};

type StudentEngine = Local<HeapTableManager>;

/// The SQL student test runner.
///
/// Holds an execution engine session, which executes the SQL statements provided to it.
pub struct SqlStudentRunner<'run> {
    /// A session from the query engine whose behavior we're testing.
    execution: RefCell<Session<'run, StudentEngine>>,
}

impl<'a> SqlStudentRunner<'a> {
    pub(crate) fn new(execution_engine: &'a StudentEngine) -> Self {
        Self {
            execution: RefCell::new(execution_engine.session()),
        }
    }

    /// Applies the function on the runner, typically to execute a series of SQL statements.
    pub(crate) fn bind<F>(&mut self, mut f: F) -> &mut Self
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self
    }

    /// Executes the input as a SQL statement, e.g. INSERT INTO table_name VALUES (...),
    /// from the `execution` session.
    pub(crate) fn execute(&mut self, input: &str) -> &mut Self {
        {
            let session = &mut self.execution.borrow_mut();
            session.execute(input).unwrap();
        }
        self
    }

    /// Executes a SQL SELECT statement from the `execution` session and verifies that
    /// its return value matches the given expected output.
    ///
    /// The expected output of a SELECT statement should be formatted as follows:
    /// - Lines are separated by a semicolon and elements of each line are separated by
    ///   a comma.
    /// - The first line is the expected column names in order, e.g. table.column, column2
    /// - Each subsequent line is the next expected row in the output, e.g. true, Jake
    pub(crate) fn select_expect(&mut self, input: &str, expected: &str) -> &mut Self {
        {
            let session = &mut self.execution.borrow_mut();
            handle(session.execute(input).unwrap(), expected)
        }
        self
    }

    /// Execute the given "testscripts/`script_name`.sql" script, typically to perform
    /// table initialization/setup tasks. (Hence the name.)
    ///
    /// Note that statements in the .sql scripts must end in semicolons.
    pub(crate) fn initialize(&mut self, script_name: &str) -> &mut Self {
        let contents = open_script(script_name).unwrap();
        parse_script(&contents).iter().for_each(|statement| {
            let session = &mut self.execution.borrow_mut();
            session.execute(statement).unwrap();
        });
        self
    }
}

/// Create a heap file based storage engine utilizing a memory buffered disk storage access.
pub fn create_storage_engine() -> HeapTableManager {
    let disk_manager = DiskManager::new("sql-test-file");
    let bpm = Arc::new(RwLock::new(
        BufferPoolManager::builder()
            .disk_manager(Arc::new(RwLock::new(disk_manager)))
            .pool_size(500)
            .replacer_k(5)
            .build(),
    ));
    HeapTableManager::new(&bpm)
}

pub fn handle(result: StatementResult, expected: &str) {
    match result {
        StatementResult::Select { columns, rows } => {
            let lines = expected.split(";").map(&str::trim).collect::<Vec<&str>>();
            let (expected_columns, expected_rows) = lines.split_at(1);

            let expected_rows: Vec<&str> = expected_rows.iter().filter(|row| !row.is_empty()).cloned().collect();
            assert_eq!(expected_rows.len(), rows.len());

            // Check that the output schema has expected column names and ordering.
            assert_eq!(
                columns
                    .into_iter()
                    .map(|c| format!("{}", c))
                    .join(", ")
                    .trim(),
                expected_columns.into_iter().join(", ").trim()
            );
            // Check that the output rows match the expected rows.
            rows.into_iter()
                .map(|r| r.to_string(None))
                .into_iter()
                .zip(expected_rows.iter())
                .into_iter()
                .for_each(|(row, expected_row)| {
                    assert_eq!(&row, &expected_row.split(",").map(&str::trim).join(", "))
                });
        }
        _ => {
            panic!("Input should be a SELECT statement.")
        }
    }
}

pub fn open_script(script_name: &str) -> Result<String, Error> {
    let file = File::open(format!("./src/sql/tests/testscripts/{}.sql", script_name))?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();

    buf_reader.read_to_string(&mut contents)?;
    Ok(contents)
}

pub fn parse_script(script: &str) -> Vec<&str> {
    script
        .split(";")
        .map(|statement| statement.trim())
        .filter(|statement| !statement.is_empty())
        .collect()
}
