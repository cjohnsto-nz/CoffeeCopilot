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
        logger.info("Initializing database...")
        if os.path.exists('coffee_data.db'):
            os.remove('coffee_data.db')
        
        # Step 2: Scrape Products
        logger.info("Scraping products from roasters...")
        scrape_products()  # This function handles its own database initialization
        
        # Step 3: Log product counts
        session = get_session()
        product_count = session.execute(text('SELECT COUNT(*) FROM products')).scalar()
        variant_count = session.execute(text('SELECT COUNT(*) FROM variants')).scalar()
        beans_count = session.execute(text('SELECT COUNT(*) FROM whole_beans_view')).scalar()
        logger.info(f"Database contains:")
        logger.info(f"- {product_count} products")
        logger.info(f"- {variant_count} variants")
        logger.info(f"- {beans_count} whole bean products")
        
        # Step 4: Enhance Products
        logger.info("Enhancing whole bean products with AI...")
        enhance_products()
        
        # Step 5: Log enhancement results
        enhanced_count = session.execute(text('SELECT COUNT(*) FROM product_extended_details')).scalar()
        logger.info(f"Enhanced {enhanced_count} products with AI")
        
        # Calculate total runtime
        runtime = time.time() - start_time
        logger.info(f"Pipeline completed in {runtime:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    run_pipeline()
