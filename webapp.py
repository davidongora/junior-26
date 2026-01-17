"""
Simple Web Application Demo for RDBMS
A todo list application using Flask

This demo now uses parameterized queries for all database operations,
providing proper SQL injection prevention.
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
from rdbms import RDBMS
import os
import re

app = Flask(__name__)
rdbms = RDBMS(db_name="webapp_db", data_dir="./data")


def validate_integer(value, default=0):
    """Validate and convert to integer"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def validate_boolean(value, default=False):
    """Validate and convert to boolean"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes']
    return bool(value) if value is not None else default

# Initialize database with tables
def init_db():
    """Initialize database tables if they don't exist"""
    # Check if tables exist
    success, tables = rdbms.execute("SHOW TABLES")
    
    if 'todos' not in tables:
        # Create todos table
        create_table_sql = """
        CREATE TABLE todos (
            id INT PRIMARY KEY,
            title VARCHAR(200),
            description VARCHAR(500),
            completed BOOLEAN,
            priority INT
        )
        """
        rdbms.execute(create_table_sql)
        print("Created todos table")
        
        # Insert some sample data using parameterized queries
        sample_todos = [
            (1, "Learn RDBMS", "Understand how relational databases work", False, 1),
            (2, "Build web app", "Create a web application using Flask", False, 2),
            (3, "Test CRUD operations", "Test all Create, Read, Update, Delete operations", False, 1)
        ]
        
        for todo in sample_todos:
            rdbms.execute(
                "INSERT INTO todos (id, title, description, completed, priority) VALUES (?, ?, ?, ?, ?)",
                list(todo)
            )
        print("Inserted sample todos")


@app.route('/')
def index():
    """Home page showing all todos"""
    success, todos = rdbms.execute("SELECT * FROM todos")
    
    if not success:
        todos = []
    
    return render_template('index.html', todos=todos)


@app.route('/api/todos', methods=['GET'])
def get_todos():
    """API endpoint to get all todos"""
    success, todos = rdbms.execute("SELECT * FROM todos")
    
    if success:
        return jsonify({'success': True, 'todos': todos})
    else:
        return jsonify({'success': False, 'error': todos}), 400


@app.route('/api/todos/<int:todo_id>', methods=['GET'])
def get_todo(todo_id):
    """API endpoint to get a specific todo"""
    success, todos = rdbms.execute("SELECT * FROM todos WHERE id = ?", [todo_id])
    
    if success and todos:
        return jsonify({'success': True, 'todo': todos[0]})
    else:
        return jsonify({'success': False, 'error': 'Todo not found'}), 404


@app.route('/api/todos', methods=['POST'])
def create_todo():
    """API endpoint to create a new todo"""
    data = request.get_json()
    
    # Validate inputs
    title = data.get('title', '')
    description = data.get('description', '')
    completed = validate_boolean(data.get('completed', False))
    priority = validate_integer(data.get('priority', 3))
    
    # Validate priority range
    if priority < 1 or priority > 3:
        priority = 3
    
    # Get next ID
    success, todos = rdbms.execute("SELECT * FROM todos")
    if success:
        next_id = max([t.get('id', 0) for t in todos], default=0) + 1
    else:
        next_id = 1
    
    # Use parameterized query
    success, result = rdbms.execute(
        "INSERT INTO todos (id, title, description, completed, priority) VALUES (?, ?, ?, ?, ?)",
        [next_id, title, description, completed, priority]
    )
    
    if success:
        return jsonify({'success': True, 'message': result, 'id': next_id}), 201
    else:
        return jsonify({'success': False, 'error': result}), 400


@app.route('/api/todos/<int:todo_id>', methods=['PUT'])
def update_todo(todo_id):
    """API endpoint to update a todo"""
    data = request.get_json()
    
    # Build UPDATE query with parameterized values
    set_parts = []
    params = []
    
    if 'title' in data:
        set_parts.append("title = ?")
        params.append(data['title'])
    if 'description' in data:
        set_parts.append("description = ?")
        params.append(data['description'])
    if 'completed' in data:
        set_parts.append("completed = ?")
        params.append(validate_boolean(data['completed']))
    if 'priority' in data:
        priority = validate_integer(data['priority'], 3)
        if 1 <= priority <= 3:
            set_parts.append("priority = ?")
            params.append(priority)
    
    if not set_parts:
        return jsonify({'success': False, 'error': 'No fields to update'}), 400
    
    # Add todo_id to params for WHERE clause
    params.append(todo_id)
    sql = f"UPDATE todos SET {', '.join(set_parts)} WHERE id = ?"
    success, result = rdbms.execute(sql, params)
    
    if success:
        return jsonify({'success': True, 'message': result})
    else:
        return jsonify({'success': False, 'error': result}), 400


@app.route('/api/todos/<int:todo_id>', methods=['DELETE'])
def delete_todo(todo_id):
    """API endpoint to delete a todo"""
    success, result = rdbms.execute("DELETE FROM todos WHERE id = ?", [todo_id])
    
    if success:
        return jsonify({'success': True, 'message': result})
    else:
        return jsonify({'success': False, 'error': result}), 400


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """API endpoint to get statistics"""
    success_all, all_todos = rdbms.execute("SELECT * FROM todos")
    success_completed, completed = rdbms.execute("SELECT * FROM todos WHERE completed = ?", [True])
    
    if success_all:
        total = len(all_todos)
        completed_count = len(completed) if success_completed else 0
        pending = total - completed_count
        
        return jsonify({
            'success': True,
            'stats': {
                'total': total,
                'completed': completed_count,
                'pending': pending
            }
        })
    else:
        return jsonify({'success': False, 'error': 'Failed to get stats'}), 400


if __name__ == '__main__':
    # Create templates directory
    os.makedirs('templates', exist_ok=True)
    
    # Initialize database
    init_db()
    
    print("\n" + "="*60)
    print("Todo List Web Application")
    print("Using Simple RDBMS")
    print("="*60)
    print("\nStarting server on http://127.0.0.1:5000")
    print("Press Ctrl+C to stop\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
