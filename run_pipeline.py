"""
Coffee Copilot Data Pipeline Orchestrator

This script orchestrates the entire data pipeline:
1. Scrapes coffee products from roaster websites
2. Enhances product details using AI
3. Creates database views for analysis
"""

import os
import time
from datetime import datetime
import logging
from database import init_db, get_session
from sqlalchemy import text
from app import main as scrape_products
from enhance_products import enhance_products

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

def run_pipeline():
    """Run the complete data pipeline"""
    start_time = time.time()
    logger = logging.getLogger(__name__)
    session = None
    
    try:
        # Step 1: Initialize/Reset Database
        logging.info("Initializing database...")
        init_db()
        
        # Step 2: Scrape Products
        logging.info("Scraping products from roasters...")
        scrape_products()  # This function handles its own database initialization
        
        # Get a database session
        session = get_session()
        
        try:
            # Count products in the beans view
            beans_count = session.execute(text('SELECT COUNT(*) FROM whole_beans_view')).scalar()
            logging.info(f"Found {beans_count} coffee products in the whole beans view")
            
            # Run AI extraction on all products
            logging.info("Enhancing products with AI extraction...")
            enhance_products()
            
            logging.info("Pipeline completed successfully!")
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            raise
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    run_pipeline()
