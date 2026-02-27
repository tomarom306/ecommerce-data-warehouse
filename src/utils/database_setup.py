import psycopg2
import sys
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from config.database import db_config

def execute_sql_file(filepath):
    """Execute SQL file with proper error handling"""
    # Make filepath relative to project root
    full_path = os.path.join(project_root, filepath)
    
    if not os.path.exists(full_path):
        print(f"✗ File not found: {full_path}")
        return False
    
    # Read SQL file
    with open(full_path, 'r') as file:
        sql_content = file.read().strip()
    
    if not sql_content:
        print(f"✗ File is empty: {filepath}")
        return False
    
    try:
        conn = psycopg2.connect(db_config.get_connection_string())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Split SQL into individual statements
        statements = sql_content.split(';')
        
        for statement in statements:
            statement = statement.strip()
            if statement:  # Skip empty statements
                try:
                    cursor.execute(statement)
                    print(f"  ✓ Executed statement")
                except Exception as e:
                    print(f"  ✗ Error in statement: {e}")
        
        cursor.close()
        conn.close()
        print(f"✓ Completed {filepath}\n")
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}\n")
        return False

def check_sql_files():
    """Check if SQL files exist"""
    files = [
        'sql/schema/staging_schema.sql',
        'sql/schema/warehouse_schema.sql'
    ]
    
    all_exist = True
    for file in files:
        full_path = os.path.join(project_root, file)
        if os.path.exists(full_path):
            size = os.path.getsize(full_path)
            print(f"✓ Found {file} ({size} bytes)")
        else:
            print(f"✗ Missing {file}")
            all_exist = False
    
    return all_exist

if __name__ == "__main__":
    print("=" * 60)
    print("Database Schema Setup")
    print("=" * 60)
    
    # Check files first
    print("\n1. Checking SQL files...")
    if not check_sql_files():
        print("\n✗ SQL files missing! Please create them first.")
        print("\nRun this to create the files:")
        print("  mkdir -p sql/schema")
        sys.exit(1)
    
    # Execute schema files
    print("\n2. Creating database schemas...")
    success = True
    
    if not execute_sql_file('sql/schema/staging_schema.sql'):
        success = False
    
    if not execute_sql_file('sql/schema/warehouse_schema.sql'):
        success = False
    
    if success:
        print("=" * 60)
        print("✓ Database schema created successfully!")
        print("=" * 60)
    else:
        print("=" * 60)
        print("✗ Some errors occurred. Check the output above.")
        print("=" * 60)