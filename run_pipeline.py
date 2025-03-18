"""
Coffee Copilot Data Pipeline Orchestrator

This script orchestrates the entire data pipeline:
1. Scrapes coffee products from roaster websites
2. Enhances product details using AI
3. Creates database views for analysis
4. Provides a coffee recommendation
"""

import os
import time
from datetime import datetime
import logging
from database import init_db, get_session
from sqlalchemy import text
from app import main as scrape_products
from enhance_products import enhance_products
from recommend_coffee import CoffeeRecommender
from order_manager import add_coffee_order

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
            
            # Step 3: Enhance Products
            logging.info("Enhancing products with AI extraction...")
            enhance_products()
            
            # Step 4: Get Coffee Recommendation
            logging.info("\nGetting coffee recommendation...")
            recommender = CoffeeRecommender()
            
            # Get and display spending summary
            history = recommender.get_order_history()
            spending = recommender.get_spending_summary(history)
            
            print("\nSpending Summary:")
            print("-" * 50)
            print(f"Current Month: ${spending['current_month']:.2f}")
            print(f"Last Month: ${spending['last_month']:.2f}")
            print(f"3-Month Average: ${spending['three_month_average']:.2f}")
            
            # Display recent orders
            print("\nRecent Orders:")
            print("-" * 50)
            for coffee in history[:5]:
                order_date = recommender.parse_date(coffee['order_date']).strftime('%Y-%m-%d')
                print(f"{order_date}: {coffee['roaster_name']} - {coffee['parent_title']} (${coffee['price']:.2f})")
            
            # Get and display recommendation
            print("\nRecommended Coffee:")
            print("-" * 50)
            print()  
            recommendation = recommender.get_recommendation()
            print()  
            print(recommendation)
            
            # Ask if user wants to add to order history
            print("\nWould you like to add this to your order history? (yes/no): ")
            response = input().strip().lower()
            
            if response == 'yes':
                # Extract coffee name from recommendation
                coffee_name = recommendation.split('\n')[0].strip()
                add_coffee_order(coffee_name, datetime.now())
                print(f"\nAdded {coffee_name} to order history")
            
            logging.info("Pipeline completed successfully!")
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            raise
            
    finally:
        if session:
            session.close()
        end_time = time.time()
        logging.info(f"Pipeline execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    run_pipeline()
