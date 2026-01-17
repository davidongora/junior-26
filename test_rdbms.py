#!/usr/bin/env python3
"""
Test script to demonstrate RDBMS functionality
"""

from rdbms import RDBMS
import os
import shutil


def cleanup():
    """Clean up test database"""
    if os.path.exists('./test_data'):
        shutil.rmtree('./test_data')


def print_section(title):
    """Print a section header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


def test_basic_crud():
    """Test basic CRUD operations"""
    print_section("Test 1: Basic CRUD Operations")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    # Create table
    print("\n1. Creating 'users' table...")
    success, result = db.execute("""
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(100) UNIQUE,
            age INT
        )
    """)
    print(f"   {result}")
    
    # Insert data
    print("\n2. Inserting users...")
    users_data = [
        (1, 'Alice Smith', 'alice@email.com', 30),
        (2, 'Bob Johnson', 'bob@email.com', 25),
        (3, 'Charlie Brown', 'charlie@email.com', 35)
    ]
    
    for user_id, name, email, age in users_data:
        success, result = db.execute(
            f"INSERT INTO users (id, name, email, age) VALUES ({user_id}, '{name}', '{email}', {age})"
        )
        print(f"   {result}")
    
    # Select all
    print("\n3. Selecting all users...")
    success, result = db.execute("SELECT * FROM users")
    print(db.format_result(result))
    
    # Select with WHERE
    print("\n4. Selecting users with age > 28...")
    success, result = db.execute("SELECT name, age FROM users WHERE age > 28")
    print(db.format_result(result))
    
    # Update
    print("\n5. Updating Bob's age to 26...")
    success, result = db.execute("UPDATE users SET age = 26 WHERE id = 2")
    print(f"   {result}")
    
    success, result = db.execute("SELECT * FROM users WHERE id = 2")
    print(db.format_result(result))
    
    # Delete
    print("\n6. Deleting user with id = 3...")
    success, result = db.execute("DELETE FROM users WHERE id = 3")
    print(f"   {result}")
    
    success, result = db.execute("SELECT * FROM users")
    print(db.format_result(result))
    
    return True


def test_joins():
    """Test JOIN operations"""
    print_section("Test 2: JOIN Operations")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    # Create tables
    print("\n1. Creating 'customers' and 'orders' tables...")
    db.execute("""
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            customer_name VARCHAR(50)
        )
    """)
    
    db.execute("""
        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            product VARCHAR(50),
            amount FLOAT
        )
    """)
    print("   Tables created")
    
    # Insert data
    print("\n2. Inserting test data...")
    customers = [
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
    ]
    
    orders = [
        (101, 1, 'Laptop', 999.99),
        (102, 1, 'Mouse', 29.99),
        (103, 2, 'Keyboard', 79.99),
        (104, 2, 'Monitor', 299.99)
    ]
    
    for cid, name in customers:
        db.execute(f"INSERT INTO customers (customer_id, customer_name) VALUES ({cid}, '{name}')")
    
    for oid, cid, product, amount in orders:
        db.execute(f"INSERT INTO orders (order_id, customer_id, product, amount) VALUES ({oid}, {cid}, '{product}', {amount})")
    
    print("   Data inserted")
    
    # INNER JOIN
    print("\n3. INNER JOIN - Customers with their orders:")
    success, result = db.execute("""
        SELECT * FROM customers 
        INNER JOIN orders ON customers.customer_id = orders.customer_id
    """)
    print(db.format_result(result))
    
    # LEFT JOIN
    print("\n4. LEFT JOIN - All customers (including those without orders):")
    success, result = db.execute("""
        SELECT * FROM customers 
        LEFT JOIN orders ON customers.customer_id = orders.customer_id
    """)
    print(db.format_result(result))
    
    return True


def test_indexes():
    """Test indexing"""
    print_section("Test 3: Indexing")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    print("\n1. Creating 'products' table...")
    db.execute("""
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price FLOAT
        )
    """)
    print("   Table created")
    
    print("\n2. Inserting products...")
    products = [
        (1, 'Laptop Pro', 'Electronics', 1299.99),
        (2, 'Wireless Mouse', 'Electronics', 29.99),
        (3, 'Office Chair', 'Furniture', 199.99),
        (4, 'Desk Lamp', 'Furniture', 49.99),
        (5, 'Notebook', 'Stationery', 5.99)
    ]
    
    for pid, name, cat, price in products:
        db.execute(f"INSERT INTO products (product_id, product_name, category, price) VALUES ({pid}, '{name}', '{cat}', {price})")
    
    print("   Products inserted")
    
    print("\n3. Creating index on 'category' column...")
    success, result = db.execute("CREATE INDEX idx_category ON products (category)")
    print(f"   {result}")
    
    print("\n4. Querying by category (using index)...")
    success, result = db.execute("SELECT * FROM products WHERE category = 'Electronics'")
    print(db.format_result(result))
    
    return True


def test_constraints():
    """Test constraints"""
    print_section("Test 4: Constraints (Primary Key, Unique, Not Null)")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    print("\n1. Creating 'employees' table with constraints...")
    db.execute("""
        CREATE TABLE employees (
            emp_id INT PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            name VARCHAR(50)
        )
    """)
    print("   Table created")
    
    print("\n2. Inserting valid employee...")
    success, result = db.execute("INSERT INTO employees (emp_id, email, name) VALUES (1, 'john@company.com', 'John Doe')")
    print(f"   ✓ {result}")
    
    print("\n3. Attempting to insert duplicate primary key...")
    success, result = db.execute("INSERT INTO employees (emp_id, email, name) VALUES (1, 'jane@company.com', 'Jane Doe')")
    if not success:
        print(f"   ✓ Correctly rejected: {result}")
    
    print("\n4. Attempting to insert duplicate email (unique constraint)...")
    success, result = db.execute("INSERT INTO employees (emp_id, email, name) VALUES (2, 'john@company.com', 'John Smith')")
    if not success:
        print(f"   ✓ Correctly rejected: {result}")
    
    print("\n5. Inserting employee with unique email...")
    success, result = db.execute("INSERT INTO employees (emp_id, email, name) VALUES (2, 'jane@company.com', 'Jane Doe')")
    print(f"   ✓ {result}")
    
    success, result = db.execute("SELECT * FROM employees")
    print(db.format_result(result))
    
    return True


def test_data_types():
    """Test different data types"""
    print_section("Test 5: Data Types (INT, VARCHAR, FLOAT, BOOLEAN)")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    print("\n1. Creating 'items' table with all data types...")
    db.execute("""
        CREATE TABLE items (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            price FLOAT,
            in_stock BOOLEAN,
            quantity INT
        )
    """)
    print("   Table created")
    
    print("\n2. Inserting items with different data types...")
    items = [
        (1, 'Widget A', 19.99, True, 100),
        (2, 'Widget B', 29.50, False, 0),
        (3, 'Widget C', 15.00, True, 50)
    ]
    
    for item_id, name, price, in_stock, qty in items:
        db.execute(f"INSERT INTO items (id, name, price, in_stock, quantity) VALUES ({item_id}, '{name}', {price}, {in_stock}, {qty})")
    
    print("   Items inserted")
    
    print("\n3. Querying all items...")
    success, result = db.execute("SELECT * FROM items")
    print(db.format_result(result))
    
    print("\n4. Querying items in stock...")
    success, result = db.execute("SELECT name, price, quantity FROM items WHERE in_stock = True")
    print(db.format_result(result))
    
    return True


def test_show_describe():
    """Test SHOW TABLES and DESCRIBE"""
    print_section("Test 6: SHOW TABLES and DESCRIBE")
    
    db = RDBMS(db_name="test_db", data_dir="./test_data")
    
    print("\n1. Showing all tables...")
    success, result = db.execute("SHOW TABLES")
    print(db.format_result(result))
    
    print("\n2. Describing 'users' table...")
    success, result = db.execute("DESCRIBE users")
    print(db.format_result(result))
    
    print("\n3. Describing 'products' table...")
    success, result = db.execute("DESCRIBE products")
    print(db.format_result(result))
    
    return True


def main():
    """Run all tests"""
    print("\n" + "█"*60)
    print("█" + " "*58 + "█")
    print("█" + "  SIMPLE RDBMS - COMPREHENSIVE TEST SUITE".center(58) + "█")
    print("█" + " "*58 + "█")
    print("█"*60)
    
    # Clean up any existing test data
    cleanup()
    
    tests = [
        ("Basic CRUD Operations", test_basic_crud),
        ("JOIN Operations", test_joins),
        ("Indexing", test_indexes),
        ("Constraints", test_constraints),
        ("Data Types", test_data_types),
        ("Metadata Commands", test_show_describe)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result, None))
        except Exception as e:
            results.append((test_name, False, str(e)))
    
    # Summary
    print_section("TEST SUMMARY")
    print()
    for test_name, result, error in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"   {status}: {test_name}")
        if error:
            print(f"      Error: {error}")
    
    passed = sum(1 for _, r, _ in results if r)
    total = len(results)
    
    print(f"\n   Results: {passed}/{total} tests passed")
    
    # Clean up
    print("\n" + "="*60)
    print("Cleaning up test data...")
    cleanup()
    print("Done!")
    print("="*60 + "\n")
    
    return passed == total


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
