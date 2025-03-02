from database import get_session, Product, ProductExtendedDetails
from ai_coffee_extractor import AICoffeeExtractor
from sqlalchemy import text
from datetime import datetime
import json
import requests
from bs4 import BeautifulSoup

def get_product_html(url):
    """Get the complete HTML content from a product URL"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching URL {url}: {str(e)}")
        return None

def enhance_products():
    """Enhance all products from the whole_beans_view with AI-extracted coffee data"""
    session = get_session()
    extractor = AICoffeeExtractor()

    # Get all products from the whole_beans_view that haven't been enhanced yet
    view_query = """
    SELECT DISTINCT p.id, p.title, p.body_html, p.tags, p.url, v.parent_title
    FROM products p
    INNER JOIN whole_beans_view v ON v.product_url = p.url
    WHERE NOT EXISTS (
        SELECT 1 FROM product_extended_details ed 
        WHERE ed.product_id = p.id
    )
    """

    products = session.execute(text(view_query)).fetchall()
    total_products = len(products)
    print(f"Found {total_products} products to enhance")

    # Process each product
    for i, product in enumerate(products, 1):
        print(f"\nProcessing [{i}/{total_products}]: {product.title}")
        print(f"URL: {product.url}")
        
        # Get the complete HTML content
        scraped_html = get_product_html(product.url)
        
        # Extract coffee data using AI
        coffee_data = extractor.extract_coffee_data(
            body_html=product.body_html,
            tags=product.tags.split(',') if product.tags else [],
            scraped_html=scraped_html,  # Add the scraped HTML to the extraction
            parent_title=product.parent_title  # Add the parent title
        )
        
        print("Extracted Data:")
        print(f"Type: {'Single Origin' if coffee_data['is_single_origin'] else ('Blend' if coffee_data['is_single_origin'] == False else 'Unknown')}")
        print(f"Origin: {coffee_data['origin']['country']}, {coffee_data['origin']['region']}")
        print(f"Process: {coffee_data['processing_method']}")
        print(f"Varietals: {', '.join(coffee_data['varietals']) if coffee_data['varietals'] else 'None'}")
        print(f"Farm: {coffee_data['farm']}")
        print(f"Producer: {coffee_data['producer']}")
        print(f"Altitude: {coffee_data['altitude']}")
        print(f"Tasting Notes: {json.dumps(coffee_data['tasting_notes'], indent=2)}")
        print(f"Recommended Rest: {coffee_data['resting_period_days']} days")
        print(f"Confidence: {coffee_data['confidence_score']:.2f}")

        # Handle farm field - convert list to string if needed
        farm = coffee_data['farm']
        if isinstance(farm, list):
            farm = ', '.join(farm)

        # Create extended details
        extended_details = ProductExtendedDetails(
            product_id=product.id,
            is_single_origin=1 if coffee_data['is_single_origin'] == True else (0 if coffee_data['is_single_origin'] == False else None),
            origin_country=coffee_data['origin']['country'],
            origin_region=coffee_data['origin']['region'],
            processing_method=coffee_data['processing_method'],
            varietals=','.join(coffee_data['varietals']) if coffee_data['varietals'] else None,
            altitude=coffee_data['altitude'],
            farm=farm,
            producer=coffee_data['producer'],
            tasting_notes=coffee_data['tasting_notes'],
            resting_period_days=coffee_data['resting_period_days'],
            extraction_confidence=coffee_data['confidence_score'],
            last_updated=datetime.now()
        )
        session.add(extended_details)
        session.commit()  # Commit after each product to avoid losing progress
        
    print("\nAll products have been enhanced")

if __name__ == "__main__":
    enhance_products()
