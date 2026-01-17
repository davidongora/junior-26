"""
Simple Relational Database Management System (RDBMS)
Supports: Table creation, CRUD operations, indexing, constraints, and joins
"""

import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from collections import defaultdict
from copy import deepcopy


class DataType:
    """Supported data types"""
    INT = "INT"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"


class Column:
    """Represents a column in a table"""
    
    def __init__(self, name: str, data_type: str, length: Optional[int] = None,
                 primary_key: bool = False, unique: bool = False, nullable: bool = True):
        self.name = name
        self.data_type = data_type
        self.length = length
        self.primary_key = primary_key
        self.unique = unique
        self.nullable = nullable
    
    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate value against column constraints"""
        if value is None:
            if not self.nullable and not self.primary_key:
                return False, f"Column {self.name} cannot be NULL"
            return True, None
        
        if self.data_type == DataType.INT:
            if not isinstance(value, int):
                try:
                    int(value)
                except (ValueError, TypeError):
                    return False, f"Invalid INT value for column {self.name}"
        elif self.data_type == DataType.FLOAT:
            if not isinstance(value, (int, float)):
                try:
                    float(value)
                except (ValueError, TypeError):
                    return False, f"Invalid FLOAT value for column {self.name}"
        elif self.data_type == DataType.VARCHAR:
            if not isinstance(value, str):
                value = str(value)
            if self.length and len(value) > self.length:
                return False, f"VARCHAR value too long for column {self.name} (max {self.length})"
        elif self.data_type == DataType.BOOLEAN:
            if not isinstance(value, bool):
                if str(value).lower() not in ['true', 'false', '1', '0']:
                    return False, f"Invalid BOOLEAN value for column {self.name}"
        
        return True, None
    
    def convert(self, value: Any) -> Any:
        """Convert value to appropriate type"""
        if value is None:
            return None
        
        if self.data_type == DataType.INT:
            return int(value)
        elif self.data_type == DataType.FLOAT:
            return float(value)
        elif self.data_type == DataType.VARCHAR:
            return str(value)
        elif self.data_type == DataType.BOOLEAN:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ['true', '1']
        
        return value
    
    def to_dict(self) -> Dict:
        """Serialize column to dictionary"""
        return {
            'name': self.name,
            'data_type': self.data_type,
            'length': self.length,
            'primary_key': self.primary_key,
            'unique': self.unique,
            'nullable': self.nullable
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'Column':
        """Deserialize column from dictionary"""
        return Column(
            name=data['name'],
            data_type=data['data_type'],
            length=data.get('length'),
            primary_key=data.get('primary_key', False),
            unique=data.get('unique', False),
            nullable=data.get('nullable', True)
        )


class Index:
    """Simple index structure for faster lookups"""
    
    def __init__(self, column_name: str):
        self.column_name = column_name
        self.index: Dict[Any, List[int]] = defaultdict(list)
    
    def add(self, value: Any, row_id: int):
        """Add value to index"""
        self.index[value].append(row_id)
    
    def remove(self, value: Any, row_id: int):
        """Remove value from index"""
        if value in self.index:
            if row_id in self.index[value]:
                self.index[value].remove(row_id)
            if not self.index[value]:
                del self.index[value]
    
    def lookup(self, value: Any) -> List[int]:
        """Lookup row IDs by value"""
        return self.index.get(value, [])


class Table:
    """Represents a database table"""
    
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.column_order = [col.name for col in columns]
        self.rows: List[Dict[str, Any]] = []
        self.next_row_id = 0
        self.indexes: Dict[str, Index] = {}
        
        # Create indexes for primary key and unique columns
        for col in columns:
            if col.primary_key or col.unique:
                self.indexes[col.name] = Index(col.name)
    
    def create_index(self, column_name: str) -> bool:
        """Create an index on a column"""
        if column_name not in self.columns:
            return False
        
        if column_name in self.indexes:
            return True  # Already indexed
        
        index = Index(column_name)
        # Build index from existing data
        for row in self.rows:
            if '_row_id' in row:
                index.add(row.get(column_name), row['_row_id'])
        
        self.indexes[column_name] = index
        return True
    
    def insert(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Insert a row into the table"""
        row = {'_row_id': self.next_row_id}
        
        # Validate and convert values
        for col_name, col in self.columns.items():
            value = values.get(col_name)
            
            # Auto-generate primary key if not provided
            if col.primary_key and value is None:
                value = self.next_row_id
            
            # Validate
            valid, error = col.validate(value)
            if not valid:
                return False, error
            
            # Convert type
            if value is not None:
                value = col.convert(value)
            
            # Check unique constraint
            if value is not None and (col.primary_key or col.unique):
                if col_name in self.indexes:
                    existing = self.indexes[col_name].lookup(value)
                    if existing:
                        return False, f"Duplicate value for {col_name}"
            
            row[col_name] = value
        
        # Add to indexes
        for col_name, index in self.indexes.items():
            if col_name in row:
                index.add(row[col_name], row['_row_id'])
        
        self.rows.append(row)
        self.next_row_id += 1
        return True, None
    
    def select(self, columns: Optional[List[str]] = None,
               where: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Select rows from the table"""
        # Filter rows
        result_rows = []
        for row in self.rows:
            if where:
                match = True
                for col, condition in where.items():
                    if col not in row:
                        match = False
                        break
                    
                    row_val = row[col]
                    
                    # Check if condition is a tuple (operator, value) or just a value
                    if isinstance(condition, tuple):
                        op, val = condition
                        # Handle comparison operators
                        if op == '>':
                            if not (row_val is not None and val is not None and row_val > val):
                                match = False
                                break
                        elif op == '<':
                            if not (row_val is not None and val is not None and row_val < val):
                                match = False
                                break
                        elif op == '>=':
                            if not (row_val is not None and val is not None and row_val >= val):
                                match = False
                                break
                        elif op == '<=':
                            if not (row_val is not None and val is not None and row_val <= val):
                                match = False
                                break
                        elif op == '!=':
                            if not (row_val != val):
                                match = False
                                break
                    else:
                        # Simple equality
                        if row_val != condition:
                            match = False
                            break
                if not match:
                    continue
            result_rows.append(row)
        
        # Project columns
        if columns:
            projected = []
            for row in result_rows:
                proj_row = {col: row.get(col) for col in columns if col in self.columns}
                projected.append(proj_row)
            return projected
        else:
            # Return all columns (except internal _row_id)
            return [{k: v for k, v in row.items() if k != '_row_id'} for row in result_rows]
    
    def update(self, set_values: Dict[str, Any],
               where: Optional[Dict[str, Any]] = None) -> Tuple[int, Optional[str]]:
        """Update rows in the table"""
        count = 0
        for row in self.rows:
            if where:
                match = True
                for col, condition in where.items():
                    if col not in row:
                        match = False
                        break
                    
                    row_val = row[col]
                    
                    # Check if condition is a tuple (operator, value) or just a value
                    if isinstance(condition, tuple):
                        op, val = condition
                        if op == '>':
                            if not (row_val is not None and val is not None and row_val > val):
                                match = False
                                break
                        elif op == '<':
                            if not (row_val is not None and val is not None and row_val < val):
                                match = False
                                break
                        elif op == '>=':
                            if not (row_val is not None and val is not None and row_val >= val):
                                match = False
                                break
                        elif op == '<=':
                            if not (row_val is not None and val is not None and row_val <= val):
                                match = False
                                break
                        elif op == '!=':
                            if not (row_val != val):
                                match = False
                                break
                    else:
                        if row_val != condition:
                            match = False
                            break
                if not match:
                    continue
            
            # Validate new values
            for col_name, new_val in set_values.items():
                if col_name not in self.columns:
                    return 0, f"Unknown column {col_name}"
                
                col = self.columns[col_name]
                valid, error = col.validate(new_val)
                if not valid:
                    return 0, error
                
                # Check unique constraint
                if new_val is not None and (col.unique or col.primary_key):
                    if col_name in self.indexes:
                        existing = self.indexes[col_name].lookup(new_val)
                        # Allow update if it's the same row
                        if existing and not all(rid == row['_row_id'] for rid in existing):
                            return 0, f"Duplicate value for {col_name}"
            
            # Update indexes
            for col_name in set_values.keys():
                if col_name in self.indexes:
                    old_val = row.get(col_name)
                    self.indexes[col_name].remove(old_val, row['_row_id'])
            
            # Update row
            for col_name, new_val in set_values.items():
                if new_val is not None:
                    new_val = self.columns[col_name].convert(new_val)
                row[col_name] = new_val
            
            # Re-add to indexes
            for col_name in set_values.keys():
                if col_name in self.indexes:
                    self.indexes[col_name].add(row[col_name], row['_row_id'])
            
            count += 1
        
        return count, None
    
    def delete(self, where: Optional[Dict[str, Any]] = None) -> int:
        """Delete rows from the table"""
        to_delete = []
        for i, row in enumerate(self.rows):
            if where:
                match = True
                for col, condition in where.items():
                    if col not in row:
                        match = False
                        break
                    
                    row_val = row[col]
                    
                    # Check if condition is a tuple (operator, value) or just a value
                    if isinstance(condition, tuple):
                        op, val = condition
                        if op == '>':
                            if not (row_val is not None and val is not None and row_val > val):
                                match = False
                                break
                        elif op == '<':
                            if not (row_val is not None and val is not None and row_val < val):
                                match = False
                                break
                        elif op == '>=':
                            if not (row_val is not None and val is not None and row_val >= val):
                                match = False
                                break
                        elif op == '<=':
                            if not (row_val is not None and val is not None and row_val <= val):
                                match = False
                                break
                        elif op == '!=':
                            if not (row_val != val):
                                match = False
                                break
                    else:
                        if row_val != condition:
                            match = False
                            break
                if not match:
                    continue
            to_delete.append(i)
        
        # Remove from indexes and rows
        for i in reversed(to_delete):
            row = self.rows[i]
            for col_name, index in self.indexes.items():
                if col_name in row:
                    index.remove(row[col_name], row['_row_id'])
            del self.rows[i]
        
        return len(to_delete)
    
    def to_dict(self) -> Dict:
        """Serialize table to dictionary"""
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in [self.columns[name] for name in self.column_order]],
            'rows': [{k: v for k, v in row.items() if k != '_row_id'} for row in self.rows],
            'next_row_id': self.next_row_id,
            'indexed_columns': list(self.indexes.keys())
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'Table':
        """Deserialize table from dictionary"""
        columns = [Column.from_dict(col_data) for col_data in data['columns']]
        table = Table(data['name'], columns)
        table.next_row_id = data.get('next_row_id', 0)
        
        # Restore rows
        for row_data in data.get('rows', []):
            values = {k: v for k, v in row_data.items()}
            table.insert(values)
        
        # Restore indexes
        for col_name in data.get('indexed_columns', []):
            if col_name not in table.indexes:
                table.create_index(col_name)
        
        return table


class Database:
    """Main database class"""
    
    def __init__(self, name: str = "mydb", data_dir: str = "./data"):
        self.name = name
        self.data_dir = data_dir
        self.tables: Dict[str, Table] = {}
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        self.db_file = os.path.join(data_dir, f"{name}.json")
    
    def create_table(self, name: str, columns: List[Column]) -> Tuple[bool, Optional[str]]:
        """Create a new table"""
        if name in self.tables:
            return False, f"Table {name} already exists"
        
        self.tables[name] = Table(name, columns)
        return True, None
    
    def drop_table(self, name: str) -> bool:
        """Drop a table"""
        if name in self.tables:
            del self.tables[name]
            return True
        return False
    
    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name"""
        return self.tables.get(name)
    
    def list_tables(self) -> List[str]:
        """List all table names"""
        return list(self.tables.keys())
    
    def join(self, left_table: str, right_table: str,
             left_col: str, right_col: str,
             join_type: str = "INNER") -> List[Dict[str, Any]]:
        """Perform a join between two tables"""
        left = self.get_table(left_table)
        right = self.get_table(right_table)
        
        if not left or not right:
            return []
        
        result = []
        
        if join_type.upper() == "INNER":
            for left_row in left.rows:
                left_val = left_row.get(left_col)
                for right_row in right.rows:
                    right_val = right_row.get(right_col)
                    if left_val == right_val and left_val is not None:
                        # Combine rows, prefixing with table name if collision
                        combined = {}
                        for k, v in left_row.items():
                            if k != '_row_id':
                                key = f"{left_table}.{k}" if k in right_row else k
                                combined[key] = v
                        for k, v in right_row.items():
                            if k != '_row_id':
                                key = f"{right_table}.{k}" if k in left_row and k != '_row_id' else k
                                if key not in combined:
                                    combined[key] = v
                        result.append(combined)
        
        elif join_type.upper() == "LEFT":
            for left_row in left.rows:
                left_val = left_row.get(left_col)
                matched = False
                for right_row in right.rows:
                    right_val = right_row.get(right_col)
                    if left_val == right_val and left_val is not None:
                        matched = True
                        combined = {}
                        for k, v in left_row.items():
                            if k != '_row_id':
                                key = f"{left_table}.{k}" if k in right_row else k
                                combined[key] = v
                        for k, v in right_row.items():
                            if k != '_row_id':
                                key = f"{right_table}.{k}" if k in left_row and k != '_row_id' else k
                                if key not in combined:
                                    combined[key] = v
                        result.append(combined)
                
                if not matched:
                    # Include left row with NULLs for right columns
                    combined = {}
                    for k, v in left_row.items():
                        if k != '_row_id':
                            combined[k] = v
                    for col in right.column_order:
                        if col not in combined:
                            combined[col] = None
                    result.append(combined)
        
        return result
    
    def save(self) -> bool:
        """Save database to disk"""
        try:
            data = {
                'name': self.name,
                'tables': {name: table.to_dict() for name, table in self.tables.items()}
            }
            with open(self.db_file, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving database: {e}")
            return False
    
    def load(self) -> bool:
        """Load database from disk"""
        try:
            if not os.path.exists(self.db_file):
                return False
            
            with open(self.db_file, 'r') as f:
                data = json.load(f)
            
            self.name = data.get('name', self.name)
            self.tables = {}
            for table_name, table_data in data.get('tables', {}).items():
                self.tables[table_name] = Table.from_dict(table_data)
            
            return True
        except Exception as e:
            print(f"Error loading database: {e}")
            return False


class SQLParser:
    """Simple SQL parser for basic SQL commands"""
    
    @staticmethod
    def parse(sql: str) -> Optional[Dict[str, Any]]:
        """Parse SQL command and return structured data"""
        # Normalize whitespace - replace newlines and multiple spaces with single space
        sql = ' '.join(sql.split())
        sql = sql.strip().rstrip(';')
        
        # CREATE TABLE
        create_table_pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(create_table_pattern, sql, re.IGNORECASE | re.DOTALL)
        if match:
            table_name = match.group(1)
            columns_def = match.group(2)
            columns = SQLParser._parse_columns(columns_def)
            return {'command': 'CREATE_TABLE', 'table': table_name, 'columns': columns}
        
        # DROP TABLE
        drop_table_pattern = r'DROP\s+TABLE\s+(\w+)'
        match = re.match(drop_table_pattern, sql, re.IGNORECASE)
        if match:
            return {'command': 'DROP_TABLE', 'table': match.group(1)}
        
        # CREATE INDEX
        create_index_pattern = r'CREATE\s+INDEX\s+\w+\s+ON\s+(\w+)\s*\((\w+)\)'
        match = re.match(create_index_pattern, sql, re.IGNORECASE)
        if match:
            return {'command': 'CREATE_INDEX', 'table': match.group(1), 'column': match.group(2)}
        
        # INSERT
        insert_pattern = r'INSERT\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)'
        match = re.match(insert_pattern, sql, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            columns = [c.strip() for c in match.group(2).split(',')]
            values = SQLParser._parse_values(match.group(3))
            return {'command': 'INSERT', 'table': table_name, 'columns': columns, 'values': values}
        
        # SELECT with JOIN
        select_join_pattern = r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+(INNER|LEFT)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(select_join_pattern, sql, re.IGNORECASE)
        if match:
            columns = [c.strip() for c in match.group(1).split(',')] if match.group(1) != '*' else None
            left_table = match.group(2)
            join_type = match.group(3).upper()
            right_table = match.group(4)
            left_join_col = match.group(6)
            right_join_col = match.group(8)
            where_clause = match.group(9)
            where = SQLParser._parse_where(where_clause) if where_clause else None
            return {
                'command': 'SELECT_JOIN',
                'columns': columns,
                'left_table': left_table,
                'right_table': right_table,
                'left_col': left_join_col,
                'right_col': right_join_col,
                'join_type': join_type,
                'where': where
            }
        
        # SELECT
        select_pattern = r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$'
        match = re.match(select_pattern, sql, re.IGNORECASE)
        if match:
            columns = [c.strip() for c in match.group(1).split(',')] if match.group(1) != '*' else None
            table_name = match.group(2)
            where_clause = match.group(3)
            where = SQLParser._parse_where(where_clause) if where_clause else None
            return {'command': 'SELECT', 'table': table_name, 'columns': columns, 'where': where}
        
        # UPDATE
        update_pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$'
        match = re.match(update_pattern, sql, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            set_clause = match.group(2)
            set_values = SQLParser._parse_set(set_clause)
            where_clause = match.group(3)
            where = SQLParser._parse_where(where_clause) if where_clause else None
            return {'command': 'UPDATE', 'table': table_name, 'set': set_values, 'where': where}
        
        # DELETE
        delete_pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$'
        match = re.match(delete_pattern, sql, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            where_clause = match.group(2)
            where = SQLParser._parse_where(where_clause) if where_clause else None
            return {'command': 'DELETE', 'table': table_name, 'where': where}
        
        # SHOW TABLES
        if re.match(r'SHOW\s+TABLES', sql, re.IGNORECASE):
            return {'command': 'SHOW_TABLES'}
        
        # DESCRIBE
        describe_pattern = r'DESCRIBE\s+(\w+)'
        match = re.match(describe_pattern, sql, re.IGNORECASE)
        if match:
            return {'command': 'DESCRIBE', 'table': match.group(1)}
        
        return None
    
    @staticmethod
    def _parse_columns(columns_def: str) -> List[Column]:
        """Parse column definitions"""
        columns = []
        parts = [p.strip() for p in columns_def.split(',')]
        
        for part in parts:
            tokens = part.split()
            if len(tokens) < 2:
                continue
            
            col_name = tokens[0]
            col_type = tokens[1].upper()
            
            # Parse VARCHAR length
            length = None
            if '(' in col_type:
                col_type, length_str = col_type.split('(')
                length = int(length_str.rstrip(')'))
            
            # Parse constraints
            primary_key = 'PRIMARY' in [t.upper() for t in tokens]
            unique = 'UNIQUE' in [t.upper() for t in tokens]
            
            # Parse NOT NULL constraint
            tokens_upper = [t.upper() for t in tokens]
            if 'NOT' in tokens_upper:
                not_index = tokens_upper.index('NOT')
                # Check if NULL follows NOT
                has_null_after_not = (not_index + 1 < len(tokens_upper) and 
                                     tokens_upper[not_index + 1] == 'NULL')
                nullable = not has_null_after_not
            else:
                nullable = True
            
            columns.append(Column(col_name, col_type, length, primary_key, unique, nullable))
        
        return columns
    
    @staticmethod
    def _parse_values(values_str: str) -> List[Any]:
        """Parse VALUES clause"""
        values = []
        parts = [p.strip() for p in values_str.split(',')]
        
        for part in parts:
            # Remove quotes
            if (part.startswith("'") and part.endswith("'")) or \
               (part.startswith('"') and part.endswith('"')):
                values.append(part[1:-1])
            elif part.upper() == 'NULL':
                values.append(None)
            elif part.upper() == 'TRUE':
                values.append(True)
            elif part.upper() == 'FALSE':
                values.append(False)
            elif '.' in part:
                try:
                    values.append(float(part))
                except:
                    values.append(part)
            else:
                try:
                    values.append(int(part))
                except:
                    values.append(part)
        
        return values
    
    @staticmethod
    def _parse_where(where_clause: str) -> Dict[str, Any]:
        """Parse WHERE clause (supports =, >, <, >=, <=, !=)"""
        where = {}
        # Simple AND support
        conditions = where_clause.split(' AND ')
        
        for condition in conditions:
            condition = condition.strip()
            # Match: column operator value
            # Try different operators
            match = re.match(r'(\w+)\s*(>=|<=|!=|>|<|=)\s*(.+)', condition)
            if match:
                col = match.group(1)
                op = match.group(2)
                val_str = match.group(3).strip()
                
                # Parse value
                if (val_str.startswith("'") and val_str.endswith("'")) or \
                   (val_str.startswith('"') and val_str.endswith('"')):
                    val = val_str[1:-1]
                elif val_str.upper() == 'NULL':
                    val = None
                elif val_str.upper() == 'TRUE':
                    val = True
                elif val_str.upper() == 'FALSE':
                    val = False
                elif '.' in val_str:
                    try:
                        val = float(val_str)
                    except:
                        val = val_str
                else:
                    try:
                        val = int(val_str)
                    except:
                        val = val_str
                
                # Store as tuple (operator, value) if not equality
                if op == '=':
                    where[col] = val
                else:
                    where[col] = (op, val)
        
        return where
    
    @staticmethod
    def _parse_set(set_clause: str) -> Dict[str, Any]:
        """Parse SET clause"""
        set_values = {}
        parts = [p.strip() for p in set_clause.split(',')]
        
        for part in parts:
            match = re.match(r'(\w+)\s*=\s*(.+)', part)
            if match:
                col = match.group(1)
                val_str = match.group(2).strip()
                
                # Parse value
                if (val_str.startswith("'") and val_str.endswith("'")) or \
                   (val_str.startswith('"') and val_str.endswith('"')):
                    val = val_str[1:-1]
                elif val_str.upper() == 'NULL':
                    val = None
                elif val_str.upper() == 'TRUE':
                    val = True
                elif val_str.upper() == 'FALSE':
                    val = False
                elif '.' in val_str:
                    try:
                        val = float(val_str)
                    except:
                        val = val_str
                else:
                    try:
                        val = int(val_str)
                    except:
                        val = val_str
                
                set_values[col] = val
        
        return set_values


class RDBMS:
    """Main RDBMS interface"""
    
    def __init__(self, db_name: str = "mydb", data_dir: str = "./data"):
        self.db = Database(db_name, data_dir)
        self.db.load()
    
    def execute(self, sql: str) -> Tuple[bool, Any]:
        """Execute a SQL command"""
        parsed = SQLParser.parse(sql)
        
        if not parsed:
            return False, "Failed to parse SQL command"
        
        command = parsed['command']
        
        try:
            if command == 'CREATE_TABLE':
                success, error = self.db.create_table(parsed['table'], parsed['columns'])
                if success:
                    self.db.save()
                    return True, f"Table {parsed['table']} created successfully"
                return False, error
            
            elif command == 'DROP_TABLE':
                if self.db.drop_table(parsed['table']):
                    self.db.save()
                    return True, f"Table {parsed['table']} dropped successfully"
                return False, f"Table {parsed['table']} does not exist"
            
            elif command == 'CREATE_INDEX':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                if table.create_index(parsed['column']):
                    self.db.save()
                    return True, f"Index created on {parsed['table']}.{parsed['column']}"
                return False, f"Failed to create index"
            
            elif command == 'INSERT':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                
                values_dict = dict(zip(parsed['columns'], parsed['values']))
                success, error = table.insert(values_dict)
                if success:
                    self.db.save()
                    return True, "1 row inserted"
                return False, error
            
            elif command == 'SELECT':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                
                rows = table.select(parsed['columns'], parsed['where'])
                return True, rows
            
            elif command == 'SELECT_JOIN':
                rows = self.db.join(
                    parsed['left_table'],
                    parsed['right_table'],
                    parsed['left_col'],
                    parsed['right_col'],
                    parsed['join_type']
                )
                
                # Apply WHERE filter if present
                if parsed['where']:
                    filtered = []
                    for row in rows:
                        match = True
                        for col, condition in parsed['where'].items():
                            if col not in row:
                                match = False
                                break
                            
                            row_val = row[col]
                            
                            # Check if condition is a tuple (operator, value) or just a value
                            if isinstance(condition, tuple):
                                op, val = condition
                                if op == '>':
                                    if not (row_val is not None and val is not None and row_val > val):
                                        match = False
                                        break
                                elif op == '<':
                                    if not (row_val is not None and val is not None and row_val < val):
                                        match = False
                                        break
                                elif op == '>=':
                                    if not (row_val is not None and val is not None and row_val >= val):
                                        match = False
                                        break
                                elif op == '<=':
                                    if not (row_val is not None and val is not None and row_val <= val):
                                        match = False
                                        break
                                elif op == '!=':
                                    if not (row_val != val):
                                        match = False
                                        break
                            else:
                                if row_val != condition:
                                    match = False
                                    break
                        if match:
                            filtered.append(row)
                    rows = filtered
                
                # Project columns if specified
                if parsed['columns']:
                    projected = []
                    for row in rows:
                        proj_row = {col: row.get(col) for col in parsed['columns']}
                        projected.append(proj_row)
                    rows = projected
                
                return True, rows
            
            elif command == 'UPDATE':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                
                count, error = table.update(parsed['set'], parsed['where'])
                if error:
                    return False, error
                self.db.save()
                return True, f"{count} row(s) updated"
            
            elif command == 'DELETE':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                
                count = table.delete(parsed['where'])
                self.db.save()
                return True, f"{count} row(s) deleted"
            
            elif command == 'SHOW_TABLES':
                tables = self.db.list_tables()
                return True, tables
            
            elif command == 'DESCRIBE':
                table = self.db.get_table(parsed['table'])
                if not table:
                    return False, f"Table {parsed['table']} does not exist"
                
                schema = []
                for col_name in table.column_order:
                    col = table.columns[col_name]
                    schema.append({
                        'Column': col.name,
                        'Type': col.data_type + (f"({col.length})" if col.length else ""),
                        'Nullable': 'YES' if col.nullable else 'NO',
                        'Key': 'PRI' if col.primary_key else ('UNI' if col.unique else '')
                    })
                return True, schema
            
            else:
                return False, f"Unknown command: {command}"
        
        except Exception as e:
            return False, f"Error executing command: {str(e)}"
    
    def format_result(self, result: Any) -> str:
        """Format query result for display"""
        if isinstance(result, str):
            return result
        
        if isinstance(result, list):
            if not result:
                return "Empty set"
            
            # Check if it's a list of strings (e.g., from SHOW TABLES)
            if all(isinstance(item, str) for item in result):
                # Simple list of strings
                lines = []
                max_width = max(len(str(item)) for item in result)
                max_width = max(max_width, 10)  # Minimum width
                
                separator = "+" + "-" * (max_width + 2) + "+"
                lines.append(separator)
                for item in result:
                    lines.append("| " + str(item).ljust(max_width) + " |")
                lines.append(separator)
                
                return "\n".join(lines)
            
            # Get all unique keys
            keys = []
            for row in result:
                for key in row.keys():
                    if key not in keys:
                        keys.append(key)
            
            if not keys:
                return "Empty set"
            
            # Calculate column widths
            widths = {key: len(str(key)) for key in keys}
            for row in result:
                for key in keys:
                    val = str(row.get(key, ''))
                    widths[key] = max(widths[key], len(val))
            
            # Build table
            lines = []
            
            # Header
            header = "| " + " | ".join(str(key).ljust(widths[key]) for key in keys) + " |"
            separator = "+" + "+".join("-" * (widths[key] + 2) for key in keys) + "+"
            
            lines.append(separator)
            lines.append(header)
            lines.append(separator)
            
            # Rows
            for row in result:
                line = "| " + " | ".join(str(row.get(key, '')).ljust(widths[key]) for key in keys) + " |"
                lines.append(line)
            
            lines.append(separator)
            lines.append(f"{len(result)} row(s) returned")
            
            return "\n".join(lines)
        
        return str(result)


def repl():
    """Interactive REPL mode"""
    print("=" * 60)
    print("Simple RDBMS - Interactive Mode")
    print("=" * 60)
    print("Commands: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, JOIN")
    print("Type 'help' for examples, 'exit' or 'quit' to quit")
    print("=" * 60)
    
    rdbms = RDBMS()
    
    while True:
        try:
            sql = input("\nSQL> ").strip()
            
            if not sql:
                continue
            
            if sql.lower() in ['exit', 'quit']:
                print("Goodbye!")
                break
            
            if sql.lower() == 'help':
                print_help()
                continue
            
            success, result = rdbms.execute(sql)
            
            if success:
                print(rdbms.format_result(result))
            else:
                print(f"Error: {result}")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except EOFError:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


def print_help():
    """Print help information"""
    help_text = """
Examples:

CREATE TABLE:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)

INSERT:
  INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)

SELECT:
  SELECT * FROM users
  SELECT name, age FROM users WHERE age = 30

UPDATE:
  UPDATE users SET age = 31 WHERE id = 1

DELETE:
  DELETE FROM users WHERE id = 1

JOIN:
  SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
  SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id

CREATE INDEX:
  CREATE INDEX idx_name ON users (name)

SHOW TABLES:
  SHOW TABLES

DESCRIBE:
  DESCRIBE users
"""
    print(help_text)


if __name__ == '__main__':
    repl()
