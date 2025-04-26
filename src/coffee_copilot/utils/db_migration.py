"""
Database migration utilities for Coffee Copilot
"""
import os
import logging
from sqlalchemy import inspect, create_engine
from coffee_copilot.database import Base, engine, create_beans_view, create_available_options_view, create_order_history_view

def get_table_columns(engine, table_name):
    """Get all column names for a table"""
    inspector = inspect(engine)
    return [column['name'] for column in inspector.get_columns(table_name)]

def recreate_database():
    """Recreate the entire database (WARNING: destroys all data)"""
    # Get database path from engine
    db_path = engine.url.database
    
    # Check if database exists and remove it
    if os.path.exists(db_path):
        os.remove(db_path)
        logging.info(f"Removed existing database: {db_path}")
    
    # Create all tables from models
    Base.metadata.create_all(engine)
    logging.info("Created new database with updated schema")
    
    # Recreate views
    create_beans_view(engine)
    create_available_options_view(engine)
    create_order_history_view(engine)
    logging.info("Recreated all database views")
    
    return True

def backup_database():
    """Create a backup of the current database"""
    import shutil
    from datetime import datetime
    
    # Get database path from engine
    db_path = engine.url.database
    
    if not os.path.exists(db_path):
        logging.warning(f"No database found at {db_path}")
        return False
    
    # Create backup with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.{timestamp}.bak"
    shutil.copy2(db_path, backup_path)
    logging.info(f"Created database backup: {backup_path}")
    
    return backup_path

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create backup before migration
    backup_path = backup_database()
    if backup_path:
        logging.info(f"Database backed up to: {backup_path}")
    
    # Recreate database with new schema
    recreate_database()
    logging.info("Database migration completed successfully")
