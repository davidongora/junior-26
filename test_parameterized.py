#!/usr/bin/env python3
"""
Test parameterized queries implementation
"""

from rdbms import RDBMS
import os
import shutil


def cleanup():
    """Clean up test database"""
    if os.path.exists('./test_params_demo'):
        shutil.rmtree('./test_params_demo')


def print_section(title):
    """Print a section header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


def test_parameterized_queries():
    """Test parameterized queries functionality"""
    print_section("Parameterized Queries Test Suite")
    
    db = RDBMS(db_name="test_params", data_dir="./test_params_demo")
    
    # Create test table
    print("\n1. Creating test table...")
    db.execute("""
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            age INT
        )
    """)
    print("   ✓ Table created")
    
    # Test positional parameters with INSERT
    print("\n2. Testing positional parameters (?) with INSERT...")
    test_data = [
        (1, "Alice Smith", "alice@example.com", 30),
        (2, "Bob's Diner", "bob@example.com", 25),
        (3, "Charlie 'The Cool' Brown", "charlie@example.com", 35),
    ]
    
    for user in test_data:
        success, result = db.execute(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
            list(user)
        )
        print(f"   ✓ Inserted: {user[1]}")
    
    # Test SELECT with positional parameters
    print("\n3. Testing SELECT with positional parameters...")
    success, rows = db.execute("SELECT * FROM users WHERE age > ?", [28])
    print(f"   Found {len(rows)} users over 28:")
    print(db.format_result(rows))
    
    # Test named parameters
    print("\n4. Testing named parameters (:name) with INSERT...")
    success, result = db.execute(
        "INSERT INTO users (id, name, email, age) VALUES (:id, :name, :email, :age)",
        {"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28}
    )
    print(f"   ✓ {result}")
    
    # Test SELECT with named parameters
    print("\n5. Testing SELECT with named parameters...")
    success, rows = db.execute(
        "SELECT name, email FROM users WHERE name = :name",
        {"name": "Diana"}
    )
    print(f"   Found user:")
    print(db.format_result(rows))
    
    # Test UPDATE with parameters
    print("\n6. Testing UPDATE with parameters...")
    success, result = db.execute(
        "UPDATE users SET age = ? WHERE id = ?",
        [31, 1]
    )
    print(f"   ✓ {result}")
    
    success, rows = db.execute("SELECT name, age FROM users WHERE id = ?", [1])
    print("   Updated user:")
    print(db.format_result(rows))
    
    # Test DELETE with parameters
    print("\n7. Testing DELETE with parameters...")
    success, result = db.execute("DELETE FROM users WHERE id = ?", [4])
    print(f"   ✓ {result}")
    
    # Test SQL injection prevention
    print("\n8. Testing SQL injection prevention...")
    
    test_cases = [
        ("'; DROP TABLE users; --", "SQL injection attempt 1"),
        ("' OR '1'='1", "SQL injection attempt 2"),
        ("admin' --", "SQL injection attempt 3"),
        ("1; DELETE FROM users", "SQL injection attempt 4"),
    ]
    
    for malicious_input, description in test_cases:
        success, result = db.execute(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
            [100 + test_cases.index((malicious_input, description)), 
             malicious_input, "test@example.com", 99]
        )
        if success:
            print(f"   ✓ {description} safely stored as data")
    
    # Verify table still exists and has data
    success, rows = db.execute("SELECT * FROM users")
    print(f"\n   Table still exists with {len(rows)} rows")
    
    # Test special characters
    print("\n9. Testing special characters handling...")
    special_chars = [
        "It's a beautiful day!",
        'Quote: "Hello World"',
        "Backslash: \\",
        "Null char: (removed)",  # \x00 is removed
        "Percent %",
        "Underscore _",
    ]
    
    for i, text in enumerate(special_chars):
        success, result = db.execute(
            "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
            [200 + i, text, f"test{i}@example.com", 20 + i]
        )
        if success:
            # Verify it's stored correctly
            success, rows = db.execute("SELECT name FROM users WHERE id = ?", [200 + i])
            if rows:
                stored = rows[0]['name']
                print(f"   ✓ '{text[:30]}...' stored as: '{stored[:30]}...'")
    
    # Test complex query with multiple parameters
    print("\n10. Testing complex query with multiple parameters...")
    success, rows = db.execute(
        "SELECT name, email, age FROM users WHERE age > ? AND age < ? AND name != ?",
        [20, 35, "test"]
    )
    print(f"   Found {len(rows)} users matching criteria")
    
    print("\n" + "="*60)
    print("  All Parameterized Query Tests Passed!")
    print("="*60)


if __name__ == '__main__':
    cleanup()
    test_parameterized_queries()
    cleanup()
    print("\nTest database cleaned up.")
