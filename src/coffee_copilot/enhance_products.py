from coffee_copilot.database import get_session, Product, ProductExtendedDetails, ProductImage
from coffee_copilot.ai_coffee_extractor import AICoffeeExtractor
from sqlalchemy import text
from datetime import datetime
import requests
import logging

def get_product_html(url):
    """Get the complete HTML content from a product URL
    
    Returns:
        tuple: (html_content, status_code) or (None, status_code) if error
    """
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text, 200
        else:
            logging.warning(f"Error fetching URL {url}: HTTP {response.status_code}")
            return None, response.status_code
    except Exception as e:
        logging.error(f"Error fetching URL {url}: {str(e)}")
        return None, 0  # 0 indicates a connection error

def enhance_products():
    """Enhance all products from the whole_beans_view with AI-extracted coffee data"""
    session = get_session()
    extractor = AICoffeeExtractor()

    # Get all products from the whole_beans_view that haven't been enhanced yet
    # Exclude products that already have extended details or are blacklisted
    # Use URL as the key to check if a product has already been enhanced
    view_query = """
    SELECT DISTINCT v.parent_title, v.product_url, p.body_html, p.tags, p.id as product_id
    FROM whole_beans_view v
    JOIN products p ON v.product_id = p.id
    WHERE NOT EXISTS (
        -- Exclude products where any product with the same URL has extended details
        SELECT 1 FROM products p2
        JOIN product_extended_details ped ON p2.id = ped.product_id
        WHERE p2.url = p.url AND ped.extraction_confidence IS NOT NULL
    )
    AND NOT EXISTS (
        -- Exclude products where any product with the same URL is blacklisted
        SELECT 1 FROM products p3
        JOIN product_extended_details ped ON p3.id = ped.product_id
        WHERE p3.url = p.url AND ped.is_blacklisted = 1
    )
    ORDER BY v.parent_title
    """

    products = session.execute(text(view_query)).fetchall()
    total_products = len(products)
    print(f"Found {total_products} products to enhance")

    # Process each product
    for i, product in enumerate(products, 1):
        print(f"\nProcessing [{i}/{total_products}]: {product.parent_title}")
        print(f"URL: {product.product_url}")
        print(f"Body HTML length: {len(product.body_html) if product.body_html else 0}")  # Debug line
        
        # Get the complete HTML content
        scraped_html, status_code = get_product_html(product.product_url)
        
        # Get the product record
        db_product = session.query(Product).filter(Product.url == product.product_url).first()
        if not db_product:
            print(f"Error: Could not find product with URL {product.product_url}")
            continue
            
        # Auto-blacklist 404 products
        if status_code == 404:
            print(f"Auto-blacklisting product due to 404 error: {product.parent_title}")
            
            # Check if there's an existing record
            existing = session.query(ProductExtendedDetails).filter_by(product_id=db_product.id).first()
            if not existing:
                # Create a new record with is_blacklisted=True
                extended_details = ProductExtendedDetails(
                    product_id=db_product.id,
                    is_blacklisted=True,
                    blacklist_reason="Auto-blacklisted due to 404 Not Found error",
                    last_updated=datetime.now()
                )
                session.add(extended_details)
                session.commit()
            else:
                # Update existing record
                existing.is_blacklisted = True
                existing.blacklist_reason = "Auto-blacklisted due to 404 Not Found error"
                existing.last_updated = datetime.now()
                session.commit()
                
            # Skip further processing for this product
            continue
            
        # Get the first product image URL if available
        first_image = session.query(ProductImage).filter(
            ProductImage.product_id == db_product.id,
            ProductImage.position == 1
        ).first()
        image_url = first_image.src if first_image else None

        try:
            # Extract coffee data using AI
            coffee_data = extractor.extract_coffee_data(
                body_html=product.body_html,
                tags=product.tags.split(',') if product.tags else [],
                scraped_html=scraped_html,
                parent_title=product.parent_title,
                image_url=image_url
            )
        except Exception as e:
            print(f"Error extracting coffee data: {str(e)}")
            coffee_data = extractor._get_empty_result()
        
        print_extracted_data(product, coffee_data)
        
        # Store the extended details
        store_extended_details(db_product, coffee_data, session)
        session.commit()  # Commit after each product to avoid losing progress
        
    print("\nAll products have been enhanced")

def print_extracted_data(product, coffee_data):
    """Print extracted coffee data in a readable format"""
    print(f"\nProcessing: {product.parent_title}")
    print(f"URL: {product.product_url}")
    print()
    
    print("Raw coffee_data:")
    print(coffee_data)
    print()
    
    print("Extracted Data:")
    print(f"Type: {'Single Origin' if coffee_data.get('is_single_origin') else ('Blend' if coffee_data.get('is_single_origin') == False else 'Unknown')}")
    print(f"Origin: {coffee_data.get('origin', {}).get('country')}, {coffee_data.get('origin', {}).get('region')}")
    print(f"Roast Level: {coffee_data.get('roast_level')}")
    print(f"Process: {coffee_data.get('processing_method')}")
    
    # Handle varietals that might be None
    varietals = coffee_data.get('varietals')
    if varietals:
        if isinstance(varietals, str):
            print(f"Varietals: {varietals}")
        else:
            print(f"Varietals: {', '.join(varietals)}")
    else:
        print("Varietals: None")
        
    print(f"Farm: {coffee_data.get('farm')}")
    print(f"Producer: {coffee_data.get('producer')}")
    print(f"Altitude: {coffee_data.get('altitude')}")
    
    # Print categorized tasting notes
    tasting_notes = coffee_data.get('tasting_notes', {})
    print("Tasting Notes:")
    for category in ['fruits', 'sweets', 'florals', 'spices', 'others']:
        notes = tasting_notes.get(category, [])
        if notes:
            print(f"  {category.title()}: {', '.join(notes)}")
        
    print(f"Recommended Rest: {coffee_data.get('resting_period_days')} days")
    print(f"Confidence: {coffee_data.get('confidence_score', 0.0):.2f}\n")

def store_extended_details(product, coffee_data, session):
    """Store the extended details, handling missing or empty data safely"""
    # Check if there's an existing record
    existing = session.query(ProductExtendedDetails).filter_by(product_id=product.id).first()
    if existing:
        session.delete(existing)
        session.commit()
    
    # Handle farm data - if it's a list, join with commas
    farm = coffee_data.get('farm')
    if isinstance(farm, list):
        farm = ', '.join(farm)
    
    # Store the extended details
    extended_details = ProductExtendedDetails(
        product_id=product.id,
        is_single_origin=1 if coffee_data.get('is_single_origin') == True else (0 if coffee_data.get('is_single_origin') == False else None),
        origin_country=coffee_data.get('origin', {}).get('country'),
        origin_region=coffee_data.get('origin', {}).get('region'),
        roast_level=coffee_data.get('roast_level'),
        processing_method=coffee_data.get('processing_method'),
        varietals=','.join(coffee_data.get('varietals', [])) if coffee_data.get('varietals') else None,
        altitude=coffee_data.get('altitude'),
        farm=farm,
        producer=coffee_data.get('producer'),
        tasting_notes=coffee_data.get('tasting_notes'),
        resting_period_days=coffee_data.get('resting_period_days'),
        extraction_confidence=coffee_data.get('confidence_score', 0.0),
        last_updated=datetime.now()
    )
    session.add(extended_details)

def enhance_single_product(product_id):
    """Enhance a single product with AI-extracted coffee data"""
    session = get_session()
    extractor = AICoffeeExtractor()
    
    # Get the product
    product = session.query(Product).filter(Product.id == product_id).first()
    if not product:
        print(f"Product with ID {product_id} not found")
        return False
    
    # Check if any product with the same URL already has extended details and is not blacklisted
    url_has_details = session.query(ProductExtendedDetails).join(Product).filter(
        Product.url == product.url,
        ProductExtendedDetails.is_blacklisted != True,
        ProductExtendedDetails.extraction_confidence != None
    ).first()
    
    if url_has_details:
        print(f"A product with URL {product.url} already has extended details")
        return True
    
    print(f"\nProcessing: {product.title}")
    print(f"URL: {product.url}")
    
    # Get the complete HTML content
    scraped_html, status_code = get_product_html(product.url)
    
    # Auto-blacklist 404 products
    if status_code == 404:
        print(f"Auto-blacklisting product due to 404 error: {product.title}")
        
        # Check if there's an existing record
        existing = session.query(ProductExtendedDetails).filter_by(product_id=product.id).first()
        if not existing:
            # Create a new record with is_blacklisted=True
            extended_details = ProductExtendedDetails(
                product_id=product.id,
                is_blacklisted=True,
                blacklist_reason="Auto-blacklisted due to 404 Not Found error",
                last_updated=datetime.now()
            )
            session.add(extended_details)
            session.commit()
            print("Product has been blacklisted.")
            return False
        else:
            # Update existing record
            existing.is_blacklisted = True
            existing.blacklist_reason = "Auto-blacklisted due to 404 Not Found error"
            existing.last_updated = datetime.now()
            session.commit()
            print("Product has been blacklisted.")
            return False
    
    # Get the first product image URL if available
    first_image = session.query(ProductImage).filter(
        ProductImage.product_id == product.id,
        ProductImage.position == 1
    ).first()
    image_url = first_image.src if first_image else None
    
    if image_url:
        print(f"Image URL: {image_url}")
    else:
        print("No image found")

    try:
        # Extract coffee data using AI
        coffee_data = extractor.extract_coffee_data(
            body_html=product.body_html,
            tags=product.tags.split(',') if product.tags else [],
            scraped_html=scraped_html,
            parent_title=product.title,
            image_url=image_url
        )
    except Exception as e:
        print(f"Error extracting coffee data: {str(e)}")
        coffee_data = extractor._get_empty_result()
    
    print_extracted_data(product, coffee_data)
    
    # Store the extended details
    store_extended_details(product, coffee_data, session)
    session.commit()
    
    # Close the session to avoid resource leaks
    session.close()
    return True

if __name__ == "__main__":
    enhance_single_product(8)
