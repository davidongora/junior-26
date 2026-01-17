#!/usr/bin/env python3
"""
Demo script showing RDBMS capabilities
Run this to see a quick demonstration of the features
"""

from rdbms import RDBMS
import time

def print_section(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60 + "\n")

def demo():
    print_section("Simple RDBMS Demonstration")
    
    db = RDBMS(db_name="demo_db", data_dir="./data")
    
    # Demo 1: Basic Table Creation and CRUD
    print("Creating a 'products' table...")
    db.execute("""
        CREATE TABLE products (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            price FLOAT,
            category VARCHAR(50)
        )
    """)
    print("✓ Table created\n")
    
    print("Inserting products...")
    products = [
        (1, "Laptop", 999.99, "Electronics"),
        (2, "Mouse", 29.99, "Electronics"),
        (3, "Desk", 299.99, "Furniture"),
        (4, "Chair", 199.99, "Furniture"),
        (5, "Notebook", 4.99, "Stationery")
    ]
    
    for p in products:
        db.execute(f"INSERT INTO products (id, name, price, category) VALUES ({p[0]}, '{p[1]}', {p[2]}, '{p[3]}')")
    print(f"✓ Inserted {len(products)} products\n")
    
    print("All products:")
    success, result = db.execute("SELECT * FROM products")
    print(db.format_result(result))
    
    print("\nProducts in Electronics category:")
    success, result = db.execute("SELECT name, price FROM products WHERE category = 'Electronics'")
    print(db.format_result(result))
    
    print("\nExpensive items (price > 100):")
    success, result = db.execute("SELECT name, price, category FROM products WHERE price > 100")
    print(db.format_result(result))
    
    # Demo 2: Updates
    print_section("UPDATE Operations")
    
    print("Applying discount to Electronics...")
    success, result = db.execute("UPDATE products SET price = 899.99 WHERE id = 1")
    print(f"✓ {result}\n")
    
    success, result = db.execute("SELECT name, price FROM products WHERE id = 1")
    print(db.format_result(result))
    
    # Demo 3: Joins
    print_section("JOIN Operations")
    
    print("Creating 'customers' and 'orders' tables...")
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), city VARCHAR(50))")
    db.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, product_id INT, quantity INT)")
    
    db.execute("INSERT INTO customers (id, name, city) VALUES (1, 'John Doe', 'New York')")
    db.execute("INSERT INTO customers (id, name, city) VALUES (2, 'Jane Smith', 'Los Angeles')")
    
    db.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1001, 1, 1, 1)")
    db.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1002, 1, 2, 2)")
    db.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1003, 2, 3, 1)")
    
    print("✓ Sample data created\n")
    
    print("Customer orders (INNER JOIN):")
    success, result = db.execute("""
        SELECT * FROM customers 
        INNER JOIN orders ON customers.id = orders.customer_id
    """)
    print(db.format_result(result))
    
    # Demo 4: Indexing
    print_section("Indexing")
    
    print("Creating index on 'category' column...")
    success, result = db.execute("CREATE INDEX idx_category ON products (category)")
    print(f"✓ {result}\n")
    
    print("Querying by category (using index):")
    success, result = db.execute("SELECT name, price FROM products WHERE category = 'Furniture'")
    print(db.format_result(result))
    
    # Demo 5: Constraints
    print_section("Constraint Validation")
    
    print("Attempting to insert duplicate primary key...")
    success, result = db.execute("INSERT INTO products (id, name, price, category) VALUES (1, 'Duplicate', 50.0, 'Test')")
    if not success:
        print(f"✗ {result}")
        print("✓ Primary key constraint enforced!\n")
    
    # Summary
    print_section("Summary")
    
    print("Available tables:")
    success, tables = db.execute("SHOW TABLES")
    print(db.format_result(tables))
    
    print("\n\nSchema of 'products' table:")
    success, schema = db.execute("DESCRIBE products")
    print(db.format_result(schema))
    
    print("\n" + "=" * 60)
    print("  Demo Complete!")
    print("  Try the interactive REPL: python rdbms.py")
    print("  Or start the web app: python webapp.py")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    demo()
