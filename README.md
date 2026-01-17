# Simple RDBMS - Relational Database Management System

A lightweight, Python-based relational database management system with SQL-like interface and an interactive REPL mode.

## Features

### Core RDBMS Features
- **Data Types**: Support for INT, VARCHAR, FLOAT, and BOOLEAN
- **Table Management**: CREATE TABLE, DROP TABLE, DESCRIBE
- **CRUD Operations**: INSERT, SELECT, UPDATE, DELETE
- **Constraints**: PRIMARY KEY, UNIQUE, NOT NULL
- **Indexing**: Automatic indexing on primary/unique keys, manual index creation
- **Joins**: INNER JOIN and LEFT JOIN support
- **Persistence**: JSON-based storage for database state
- **REPL Mode**: Interactive SQL command-line interface

### SQL-Like Interface
Supports standard SQL syntax for common operations:
- `CREATE TABLE`, `DROP TABLE`
- `INSERT INTO ... VALUES`
- `SELECT ... FROM ... WHERE`
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`
- `INNER JOIN`, `LEFT JOIN`
- `CREATE INDEX`
- `SHOW TABLES`, `DESCRIBE`

## Installation

1. Clone the repository:
```bash
git clone https://github.com/davidongora/junior-26.git
cd junior-26
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Interactive REPL Mode

Start the interactive database shell:

```bash
python rdbms.py
```

Example session:
```sql
SQL> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)
Table users created successfully

SQL> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
1 row inserted

SQL> INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
1 row inserted

SQL> SELECT * FROM users
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 30  |
| 2  | Bob   | 25  |
+----+-------+-----+
2 row(s) returned

SQL> UPDATE users SET age = 31 WHERE id = 1
1 row(s) updated

SQL> SELECT * FROM users WHERE age > 28
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 31  |
+----+-------+-----+
1 row(s) returned

SQL> DELETE FROM users WHERE id = 2
1 row(s) deleted

SQL> SHOW TABLES
+-------+
| users |
+-------+

SQL> DESCRIBE users
+--------+--------------+----------+-----+
| Column | Type         | Nullable | Key |
+--------+--------------+----------+-----+
| id     | INT          | NO       | PRI |
| name   | VARCHAR(50)  | YES      |     |
| age    | INT          | YES      |     |
+--------+--------------+----------+-----+
```

### Programmatic Usage

```python
from rdbms import RDBMS, Column, DataType

# Initialize database
db = RDBMS(db_name="mydb", data_dir="./data")

# Execute SQL commands
success, result = db.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price FLOAT)")

if success:
    print(result)

# Insert data
db.execute("INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99)")

# Query data
success, rows = db.execute("SELECT * FROM products WHERE price > 500")
if success:
    for row in rows:
        print(row)
```

### Joins Example

```sql
SQL> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
SQL> CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product VARCHAR(50))

SQL> INSERT INTO users (id, name) VALUES (1, 'Alice')
SQL> INSERT INTO users (id, name) VALUES (2, 'Bob')

SQL> INSERT INTO orders (id, user_id, product) VALUES (1, 1, 'Laptop')
SQL> INSERT INTO orders (id, user_id, product) VALUES (2, 1, 'Mouse')
SQL> INSERT INTO orders (id, user_id, product) VALUES (3, 2, 'Keyboard')

SQL> SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
+----+-------+----+---------+----------+
| id | name  | id | user_id | product  |
+----+-------+----+---------+----------+
| 1  | Alice | 1  | 1       | Laptop   |
| 1  | Alice | 2  | 1       | Mouse    |
| 2  | Bob   | 3  | 2       | Keyboard |
+----+-------+----+---------+----------+
```

## Web Application Demo

A simple todo list web application demonstrating all CRUD operations.

### Running the Web App

```bash
python webapp.py
```

Then open your browser to: `http://127.0.0.1:5000`

### Features
- ✅ Create new tasks with title, description, and priority
- 📖 Read/view all tasks
- ✏️ Update task status (complete/reopen)
- 🗑️ Delete tasks
- 📊 Real-time statistics (total, completed, pending)

The web application uses the RDBMS to store and manage todo items, demonstrating:
- **CREATE**: Add new todos via the form
- **READ**: Display all todos and statistics
- **UPDATE**: Mark todos as complete or reopen them
- **DELETE**: Remove todos permanently

## Architecture

### Core Components

1. **Column**: Represents table columns with data type and constraints
2. **Index**: Simple B-tree-like structure for fast lookups
3. **Table**: Manages rows, columns, and indexes
4. **Database**: Container for tables with join capabilities
5. **SQLParser**: Parses SQL-like commands into structured operations
6. **RDBMS**: Main interface combining database and parser

### Storage

Data is persisted to JSON files in the `./data` directory:
- One JSON file per database
- Contains table schemas and all row data
- Loaded on initialization, saved after modifications

## Supported SQL Commands

### DDL (Data Definition Language)
- `CREATE TABLE table_name (column_name TYPE constraints, ...)`
- `DROP TABLE table_name`
- `CREATE INDEX index_name ON table_name (column_name)`
- `SHOW TABLES`
- `DESCRIBE table_name`

### DML (Data Manipulation Language)
- `INSERT INTO table_name (col1, col2) VALUES (val1, val2)`
- `SELECT col1, col2 FROM table_name WHERE condition`
- `SELECT * FROM table1 INNER JOIN table2 ON table1.col = table2.col`
- `SELECT * FROM table1 LEFT JOIN table2 ON table1.col = table2.col`
- `UPDATE table_name SET col1 = val1 WHERE condition`
- `DELETE FROM table_name WHERE condition`

### Data Types
- `INT`: Integer numbers
- `VARCHAR(length)`: Variable-length strings
- `FLOAT`: Floating-point numbers
- `BOOLEAN`: True/False values

### Constraints
- `PRIMARY KEY`: Unique identifier, auto-indexed
- `UNIQUE`: Unique values, auto-indexed
- `NOT NULL`: Cannot be null

## Examples

See the interactive REPL mode for live examples by running:
```bash
python rdbms.py
```

Type `help` in the REPL to see command examples.

## Limitations

This is a simple educational RDBMS with the following limitations:
- Single-user (no concurrent access control)
- In-memory operations (not optimized for large datasets)
- Simple query optimizer (no cost-based optimization)
- Basic WHERE clause support (equality only, simple AND)
- No transactions or ACID guarantees
- No views, stored procedures, or triggers
- JSON-based storage (not efficient for large data)

## License

MIT License
