use itertools::Itertools;
use rustydb::common::Result;
use rustydb::sql::engine::{Engine, Local, Session, StatementResult};
use rustydb::storage::buffer::buffer_pool_manager::BufferPoolManager;
use rustydb::storage::disk::disk_manager::DiskManager;
use rustydb::storage::tuple::Row;
use rustydb::storage::HeapTableManager;
use rustydb::types::field::Label;
use std::cell::RefCell;
use std::io::{stdin, stdout, Write};
use std::sync::{Arc, RwLock};

const FILENAME: &str = "main";

fn main() -> Result<()> {
    let storage = create_storage_engine();
    let engine = Local::new(storage);
    let session = RefCell::new(engine.session());

    loop {
        print!("> ");
        let command = input()?;

        if command.is_empty() {
            continue;
        };
        execute(&command, &mut session.borrow_mut())
            .unwrap_or_else(|err| println!("oops, {}", err.to_string()))
    }
}

fn execute<'a, E: Engine<'a>>(command: &str, session: &mut Session<'a, E>) -> Result<()> {
    match session.execute(command)? {
        StatementResult::Explain(_) => {
            todo!();
        }
        StatementResult::CreateTable { name } => println!("[console] Created table '{}'.", name),
        StatementResult::DropTable { name, existed } => match existed {
            true => println!("[console] Dropped table '{}'.", name),
            false => println!("[console] Table '{}' does not exist.", name),
        },
        StatementResult::Delete { count } => println!("[console] Deleted {} tuples.", count),
        StatementResult::Insert {
            count,
            record_ids: _,
        } => println!("[console] Inserted {} tuples.", count),
        StatementResult::Update { count } => println!("[console] Updated {} tuples.", count),
        StatementResult::Select { columns, rows } => {
            print_columns(&columns);
            print_rows(&rows);
        }
    }
    Ok(())
}

fn input() -> Result<String> {
    stdout().flush()?;

    let mut result = String::new();
    let mut input = String::new();
    loop {
        input.clear();
        stdin().read_line(&mut input)?;

        let trimmed = input.trim();
        match trimmed.ends_with("\\") {
            true => result.push_str(&trimmed[..trimmed.len() - 1]),
            false => {
                result.push_str(trimmed);
                break;
            }
        }
    }
    Ok(result)
}

fn create_storage_engine() -> HeapTableManager {
    let disk_manager = DiskManager::new(FILENAME);
    let bpm = Arc::new(RwLock::new(
        BufferPoolManager::builder()
            .disk_manager(Arc::new(RwLock::new(disk_manager)))
            .pool_size(500)
            .replacer_k(15)
            .build(),
    ));
    HeapTableManager::new(&bpm)
}

fn print_columns(columns: &[Label]) {
    println!("  [{}]", columns.iter().map(|c| c.to_string()).join(", "));
}

fn print_rows(rows: &[Row]) {
    rows.iter()
        .for_each(|row| println!("  {}", row.iter().map(|row| row.to_string()).join(", ")));
}
