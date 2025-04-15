# SQLDBMS

A lightweight SQL Database Management System built from scratch in Rust. This project implements essential components of a DBMS including a system catalog, storage manager, transaction manager, and query processor.

## 📦 Project Structure
SQLDBMS/ │ ├── DBMS/ # Core components of the database engine │ 
├── catalog/ # System catalog (metadata storage) │ 
├── storage/ # Page/block management and I/O │ 
├── transaction/ # Transaction handling and concurrency │ └── ...
│ ├── SQL/ # SQL parsing and query execution │
├── lexer/ # Lexical analyzer for SQL │
├── parser/ # SQL grammar parser │
├── planner/ # Logical planning of queries │ └── executor/ # Execution of parsed SQL queries │ └── README.md # Project overview (you are here!)

## 🚀 Features

- ✅ Create, read, update, and delete operations
- ✅ Basic SQL SELECT, INSERT, and DELETE statements
- ✅ In-memory page-based storage system
- ✅ Transaction support with basic isolation
- ✅ Modular design using idiomatic Rust

## 🔧 Getting Started

### Prerequisites

- Rust and Cargo installed: [Install Rust](https://www.rust-lang.org/tools/install)

### Build

 Implement JOIN operations

 Add indexing support

 Improve SQL grammar support (e.g., WHERE clauses, ORDER BY)

 Add persistence via disk-backed storage

 Build CLI shell or web-based interface

 Related Concepts
Relational Algebra

B+ Trees & Indexing

Transaction Management & Concurrency Control

SQL Parsing & Execution

Rust Ownership & Safety Guarantees
