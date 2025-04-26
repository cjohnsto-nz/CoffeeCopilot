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
from coffee_copilot.database import init_db, get_session
from sqlalchemy import text
from coffee_copilot.app import main as scrape_products
from coffee_copilot.enhance_products import enhance_products
from coffee_copilot.recommend_coffee import CoffeeRecommender
from coffee_copilot.order_manager import add_coffee_order

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'pipeline.log')),
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
            
            # Loop for handling recommendations until user is satisfied
            getting_recommendations = True
            
            while getting_recommendations:
                # Extract coffee name from recommendation
                coffee_name = recommendation.split('\n')[0].strip()
                
                # Ask if user wants to add to order history, get another recommendation, or skip
                print("\nOptions:")
                print("1. Add to order history")
                print("2. Get another recommendation")
                print("3. Skip (do nothing)")
                print("\nYour choice (1/2/3): ")
                response = input().strip().lower()
                
                if response == '1':
                    # Add to order history
                    add_coffee_order(coffee_name, datetime.now())
                    print(f"\nAdded {coffee_name} to order history")
                    getting_recommendations = False
                    
                elif response == '2':
                    # Always ask for feedback/reason regardless of blacklisting
                    print("\nWhy didn't you like this recommendation? (Press Enter to skip): ")
                    rejection_reason = input().strip()
                    
                    # Store the rejected recommendation
                    if coffee_name not in [rec[0] for rec in recommender.rejected_recommendations]:
                        recommender.rejected_recommendations.append((coffee_name, rejection_reason))
                    
                    # Ask if user wants to blacklist this coffee
                    print("\nWould you like to blacklist this coffee? (yes/no): ")
                    blacklist_response = input().strip().lower()
                    
                    if blacklist_response == 'yes':
                        # Blacklist the coffee using the already provided reason
                        from coffee_copilot.order_manager import blacklist_coffee
                        blacklist_coffee(coffee_name, rejection_reason)
                        print(f"\nBlacklisted {coffee_name}")
                    else:
                        print("\nCoffee not blacklisted")
                    
                    # Get a new recommendation, passing the rejection reason
                    print("\nGetting another recommendation...\n")
                    recommendation = recommender.get_recommendation(rejection_reason)
                    print(recommendation)
                    
                    # Continue the loop to handle the new recommendation
                    
                else:  # response == '3' or any other input
                    # Skip
                    print("\nSkipped. No changes made.")
                    getting_recommendations = False
            
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
