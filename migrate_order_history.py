#!/usr/bin/env python
"""
Migrate order history from the old database to the new database.
This script:
1. Connects to both old and new databases
2. Retrieves all order history records from the old database
3. Matches products and variants in the new database
4. Creates products and variants that no longer exist if needed
5. Migrates order history records to the new database
"""

import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, JSON, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to data directory
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

# Database paths
old_db_path = os.path.join(data_dir, 'coffee_data.db')
new_db_path = os.path.join(data_dir, 'coffee_data_new.db')

# Check if both databases exist
if not os.path.exists(old_db_path):
    logger.error(f"Old database not found at {old_db_path}")
    sys.exit(1)

if not os.path.exists(new_db_path):
    logger.error(f"New database not found at {new_db_path}")
    sys.exit(1)

# Create engines for both databases
old_engine = create_engine(f'sqlite:///{old_db_path}', connect_args={'check_same_thread': False})
new_engine = create_engine(f'sqlite:///{new_db_path}', connect_args={'check_same_thread': False})

# Create sessions for both databases
OldSession = sessionmaker(bind=old_engine)
NewSession = sessionmaker(bind=new_engine)

# Initialize sessions
old_session = OldSession()
new_session = NewSession()

# Make NewSession available globally for error recovery
def get_new_session():
    """Get a new session for the new database"""
    return NewSession()

# Import models from coffee_copilot.database
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from coffee_copilot.database import Roaster, Product, Variant, OrderHistory, ProductExtendedDetails

def migrate_order_history():
    """Migrate order history from old database to new database"""
    logger.info("Starting order history migration")
    
    # Get all order history records from old database
    old_orders_query = """
    SELECT 
        oh.id, oh.product_id, oh.variant_id, oh.order_date, oh.quantity, oh.price_paid, oh.notes,
        oh.roaster_name, oh.product_title, oh.product_url, oh.option1, oh.option2, oh.option3,
        oh.is_single_origin, oh.origin_country, oh.origin_region, oh.roast_level, oh.processing_method,
        oh.varietals, oh.altitude, oh.farm, oh.producer, oh.tasting_notes,
        oh.created_at, oh.updated_at,
        p.title as old_product_title, p.url as old_product_url,
        v.option1 as old_option1, v.option2 as old_option2, v.option3 as old_option3
    FROM order_history oh
    LEFT JOIN products p ON oh.product_id = p.id
    LEFT JOIN variants v ON oh.variant_id = v.id
    ORDER BY oh.order_date
    """
    
    old_orders = old_session.execute(text(old_orders_query)).fetchall()
    logger.info(f"Found {len(old_orders)} order history records in old database")
    
    # Track statistics
    stats = {
        'total': len(old_orders),
        'migrated': 0,
        'product_found': 0,
        'product_created': 0,
        'variant_found': 0,
        'variant_created': 0,
        'errors': 0
    }
    
    # Process each order
    for order in old_orders:
        try:
            # Try to find the product in the new database
            new_product = None
            new_variant = None
            
            # First try to match by URL if available
            if order.product_url:
                new_product = new_session.query(Product).filter_by(url=order.product_url).first()
                if new_product:
                    logger.info(f"Found product by URL: {order.product_url}")
                    stats['product_found'] += 1
            
            # If no product found by URL, try to match by title
            if not new_product and order.product_title:
                new_product = new_session.query(Product).filter_by(title=order.product_title).first()
                if new_product:
                    logger.info(f"Found product by title: {order.product_title}")
                    stats['product_found'] += 1
            
            # If still no product found, create a new one
            if not new_product:
                logger.info(f"Creating new product for order: {order.product_title}")
                
                # Find or create roaster
                roaster = new_session.query(Roaster).filter_by(name=order.roaster_name).first()
                if not roaster:
                    roaster = Roaster(
                        name=order.roaster_name,
                        description=order.roaster_name,
                        url=f"https://example.com/{order.roaster_name.lower().replace(' ', '-')}"
                    )
                    new_session.add(roaster)
                    new_session.flush()
                
                # Convert datetime objects properly for product creation
                created_at = datetime.fromisoformat(str(order.created_at)) if order.created_at else datetime.now()
                updated_at = datetime.fromisoformat(str(order.updated_at)) if order.updated_at else datetime.now()
                
                # Create product
                new_product = Product(
                    roaster_id=roaster.id,
                    title=order.product_title,
                    url=order.product_url or f"https://example.com/{order.roaster_name.lower().replace(' ', '-')}/{order.product_title.lower().replace(' ', '-')}",
                    created_at=created_at,
                    updated_at=updated_at,
                    last_updated=datetime.now()
                )
                new_session.add(new_product)
                new_session.flush()
                stats['product_created'] += 1
                
                # Create extended details if coffee attributes are available
                if any([order.is_single_origin, order.origin_country, order.roast_level, order.processing_method]):
                    extended_details = ProductExtendedDetails(
                        product_id=new_product.id,
                        is_single_origin=order.is_single_origin,
                        origin_country=order.origin_country,
                        origin_region=order.origin_region,
                        roast_level=order.roast_level,
                        processing_method=order.processing_method,
                        varietals=order.varietals,
                        altitude=order.altitude,
                        farm=order.farm,
                        producer=order.producer,
                        tasting_notes=order.tasting_notes,
                        extraction_confidence=1.0,  # High confidence since this came from order history
                        last_updated=datetime.now()
                    )
                    new_session.add(extended_details)
            
            # Try to find the variant in the new database
            if new_product:
                # Try to match by option values
                variant_query = new_session.query(Variant).filter_by(
                    product_id=new_product.id
                )
                
                # Add option filters if available
                if order.option1:
                    variant_query = variant_query.filter_by(option1=order.option1)
                if order.option2:
                    variant_query = variant_query.filter_by(option2=order.option2)
                if order.option3:
                    variant_query = variant_query.filter_by(option3=order.option3)
                
                new_variant = variant_query.first()
                
                if new_variant:
                    logger.info(f"Found variant for product {new_product.title} with options: {order.option1}, {order.option2}, {order.option3}")
                    stats['variant_found'] += 1
                else:
                    # Create new variant
                    logger.info(f"Creating new variant for product {new_product.title} with options: {order.option1}, {order.option2}, {order.option3}")
                    
                    # Convert datetime objects properly for variant creation
                    variant_created_at = datetime.fromisoformat(str(order.created_at)) if order.created_at else datetime.now()
                    variant_updated_at = datetime.fromisoformat(str(order.updated_at)) if order.updated_at else datetime.now()
                    
                    new_variant = Variant(
                        product_id=new_product.id,
                        title=f"{order.option1} {order.option2} {order.option3}".strip(),
                        option1=order.option1,
                        option2=order.option2,
                        option3=order.option3,
                        price=order.price_paid,  # Use the price paid as the variant price
                        parent_title=new_product.title,
                        created_at=variant_created_at,
                        updated_at=variant_updated_at,
                        last_updated=datetime.now()
                    )
                    new_session.add(new_variant)
                    new_session.flush()
                    stats['variant_created'] += 1
            
            # Convert datetime objects properly
            order_date = datetime.fromisoformat(str(order.order_date)) if order.order_date else datetime.now()
            created_at = datetime.fromisoformat(str(order.created_at)) if order.created_at else datetime.now()
            updated_at = datetime.fromisoformat(str(order.updated_at)) if order.updated_at else datetime.now()
            
            # Handle tasting notes - ensure it's properly formatted as JSON
            tasting_notes = order.tasting_notes
            if isinstance(tasting_notes, str):
                try:
                    import json
                    tasting_notes = json.loads(tasting_notes)
                except:
                    tasting_notes = {"notes": [tasting_notes]} if tasting_notes else {}
            
            # Create the order history record in the new database
            new_order = OrderHistory(
                product_id=new_product.id if new_product else None,
                variant_id=new_variant.id if new_variant else None,
                order_date=order_date,
                quantity=order.quantity,
                price_paid=order.price_paid,
                notes=order.notes,
                roaster_name=order.roaster_name,
                product_title=order.product_title,
                product_url=order.product_url,
                option1=order.option1,
                option2=order.option2,
                option3=order.option3,
                is_single_origin=bool(order.is_single_origin) if order.is_single_origin is not None else None,
                origin_country=order.origin_country,
                origin_region=order.origin_region,
                roast_level=order.roast_level,
                processing_method=order.processing_method,
                varietals=order.varietals,
                altitude=order.altitude,
                farm=order.farm,
                producer=order.producer,
                tasting_notes=tasting_notes,
                created_at=created_at,
                updated_at=updated_at
            )
            new_session.add(new_order)
            stats['migrated'] += 1
            
            # Commit every 10 orders to avoid losing progress
            if stats['migrated'] % 10 == 0:
                try:
                    new_session.commit()
                    logger.info(f"Committed {stats['migrated']} orders")
                except Exception as commit_error:
                    logger.error(f"Error committing batch: {str(commit_error)}")
                    new_session.rollback()
            
        except Exception as e:
            logger.error(f"Error migrating order {order.id}: {str(e)}")
            stats['errors'] += 1
            # Rollback the session to clear any failed transactions
            try:
                new_session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {str(rollback_error)}")
                # Create a new session if rollback fails
                new_session = get_new_session()
    
    # Final commit
    try:
        new_session.commit()
        logger.info("Final commit successful")
    except Exception as e:
        logger.error(f"Error in final commit: {str(e)}")
        try:
            new_session.rollback()
            logger.info("Rolled back after final commit error")
        except Exception as rollback_error:
            logger.error(f"Error rolling back: {str(rollback_error)}")
    
    # Log statistics
    logger.info("Order history migration completed")
    logger.info(f"Total orders: {stats['total']}")
    logger.info(f"Migrated orders: {stats['migrated']}")
    logger.info(f"Products found: {stats['product_found']}")
    logger.info(f"Products created: {stats['product_created']}")
    logger.info(f"Variants found: {stats['variant_found']}")
    logger.info(f"Variants created: {stats['variant_created']}")
    logger.info(f"Errors: {stats['errors']}")

if __name__ == "__main__":
    migrate_order_history()
