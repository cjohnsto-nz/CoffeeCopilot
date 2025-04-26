from shopify_scraper import scraper
from coffee_copilot.database import init_db, get_session, Roaster, Product, ProductOption, ProductImage, Variant
from coffee_copilot.config import ROASTER_URLS, config
from datetime import datetime
import pandas as pd

def store_data(roaster_name, roaster_url, products_df, variants_df, session):
    """Store roaster, products, options, and images in the database"""
    
    # Get roaster description from config
    roaster_description = next((data['description'] for name, data in config['roasters'].items() 
                              if data['name'] == roaster_name), roaster_name.title())
    
    # Create or get roaster
    roaster = session.query(Roaster).filter_by(url=roaster_url).first()
    if not roaster:
        roaster = Roaster(name=roaster_name, description=roaster_description, url=roaster_url)
        session.add(roaster)
        session.flush()
    else:
        roaster.description = roaster_description  # Update description in case it changed
        session.flush()

    # Store products
    for _, row in products_df.iterrows():
        # Check if product with this URL already exists
        existing_product = session.query(Product).filter_by(url=row['url']).first()
        
        if existing_product:
            # Update existing product
            existing_product.title = row['title']
            existing_product.handle = row.get('handle', '')
            existing_product.body_html = row.get('body_html', '')
            existing_product.published_at = pd.to_datetime(row.get('published_at'))
            existing_product.updated_at = pd.to_datetime(row.get('updated_at'))
            existing_product.vendor = row.get('vendor', '')
            existing_product.product_type = row.get('product_type', '')
            existing_product.tags = ','.join(row['tags']) if isinstance(row.get('tags'), list) else row.get('tags', '')
            existing_product.last_updated = datetime.now()
            product = existing_product
        else:
            # Create new product
            product = Product(
                roaster_id=roaster.id,
                title=row['title'],
                handle=row.get('handle', ''),
                body_html=row.get('body_html', ''),
                published_at=pd.to_datetime(row.get('published_at')),
                created_at=pd.to_datetime(row.get('created_at')),
                updated_at=pd.to_datetime(row.get('updated_at')),
                vendor=row.get('vendor', ''),
                product_type=row.get('product_type', ''),
                tags=','.join(row['tags']) if isinstance(row.get('tags'), list) else row.get('tags', ''),
                url=row['url'],
                last_updated=datetime.now()
            )
            session.add(product)
        
        session.flush()

        # Clear existing options and images if updating an existing product
        if existing_product:
            # Delete existing options
            session.query(ProductOption).filter_by(product_id=product.id).delete()
            # Delete existing images
            session.query(ProductImage).filter_by(product_id=product.id).delete()
        
        # Store options
        if 'options' in row and isinstance(row['options'], list):
            for option in row['options']:
                opt = ProductOption(
                    product_id=product.id,
                    name=option.get('name', ''),
                    values=','.join(option['values']) if isinstance(option.get('values'), list) else str(option.get('values', ''))
                )
                session.add(opt)

        # Store images
        if 'images' in row and isinstance(row['images'], list):
            for idx, image in enumerate(row['images']):
                img = ProductImage(
                    product_id=product.id,
                    src=image.get('src', ''),
                    position=idx + 1
                )
                session.add(img)

        # Store variants for this product
        product_variants = variants_df[variants_df['parent_id'] == row['id']]
        
        # If updating existing product, update variants instead of deleting them
        if existing_product:
            # Get existing variants for this product
            existing_variants = {}
            for v in session.query(Variant).filter_by(product_id=product.id).all():
                # Create a key based on option values to match with incoming variants
                variant_key = f"{v.option1}_{v.option2}_{v.option3}"
                existing_variants[variant_key] = v
            
            # Process each variant from the scraper
            for _, variant in product_variants.iterrows():
                # Create the same key for the incoming variant
                variant_key = f"{variant.get('option1', '')}_{variant.get('option2', '')}_{variant.get('option3', '')}"
                
                if variant_key in existing_variants:
                    # Update existing variant
                    v = existing_variants[variant_key]
                    v.title = variant['title']
                    v.available = int(variant.get('available', 0))
                    v.compare_at_price = float(variant['compare_at_price']) if pd.notna(variant.get('compare_at_price')) else None
                    v.featured_image = variant.get('featured_image', '')
                    v.grams = int(variant.get('grams', 0))
                    v.position = int(variant.get('position', 0))
                    v.price = float(variant['price']) if pd.notna(variant.get('price')) else 0.0
                    v.requires_shipping = int(variant.get('requires_shipping', 1))
                    v.sku = variant.get('sku', '')
                    v.taxable = int(variant.get('taxable', 1))
                    v.updated_at = pd.to_datetime(variant.get('updated_at'))
                    v.parent_title = variant.get('parent_title', '')
                    v.vendor = variant.get('vendor', '')
                    v.weight = variant.get('weight')
                    v.weight_unit = variant.get('weight_unit')
                    v.barcode = variant.get('barcode')
                    v.inventory_quantity = variant.get('inventory_quantity', 0)
                    v.last_updated = datetime.now()
                else:
                    # Create new variant
                    v = Variant(
                        product_id=product.id,
                        title=variant['title'],
                        available=int(variant.get('available', 0)),
                        compare_at_price=float(variant['compare_at_price']) if pd.notna(variant.get('compare_at_price')) else None,
                        created_at=pd.to_datetime(variant.get('created_at')),
                        featured_image=variant.get('featured_image', ''),
                        grams=int(variant.get('grams', 0)),
                        option1=variant.get('option1', ''),
                        option2=variant.get('option2', ''),
                        option3=variant.get('option3', ''),
                        position=int(variant.get('position', 0)),
                        price=float(variant['price']) if pd.notna(variant.get('price')) else 0.0,
                        requires_shipping=int(variant.get('requires_shipping', 1)),
                        sku=variant.get('sku', ''),
                        taxable=int(variant.get('taxable', 1)),
                        updated_at=pd.to_datetime(variant.get('updated_at')),
                        parent_title=variant.get('parent_title', ''),
                        vendor=variant.get('vendor', ''),
                        weight=variant.get('weight'),
                        weight_unit=variant.get('weight_unit'),
                        barcode=variant.get('barcode'),
                        inventory_quantity=variant.get('inventory_quantity', 0),
                        last_updated=datetime.now()
                    )
                    session.add(v)
        else:
            # For new products, simply add all variants
            for _, variant in product_variants.iterrows():
                v = Variant(
                    product_id=product.id,
                    title=variant['title'],
                    available=int(variant.get('available', 0)),
                    compare_at_price=float(variant['compare_at_price']) if pd.notna(variant.get('compare_at_price')) else None,
                    created_at=pd.to_datetime(variant.get('created_at')),
                    featured_image=variant.get('featured_image', ''),
                    grams=int(variant.get('grams', 0)),
                    option1=variant.get('option1', ''),
                    option2=variant.get('option2', ''),
                    option3=variant.get('option3', ''),
                    position=int(variant.get('position', 0)),
                    price=float(variant['price']) if pd.notna(variant.get('price')) else 0.0,
                    requires_shipping=int(variant.get('requires_shipping', 1)),
                    sku=variant.get('sku', ''),
                    taxable=int(variant.get('taxable', 1)),
                    updated_at=pd.to_datetime(variant.get('updated_at')),
                    parent_title=variant.get('parent_title', ''),
                    vendor=variant.get('vendor', ''),
                    weight=variant.get('weight'),
                    weight_unit=variant.get('weight_unit'),
                    barcode=variant.get('barcode'),
                    inventory_quantity=variant.get('inventory_quantity', 0),
                    last_updated=datetime.now()
                )
                session.add(v)

def main():
    # Initialize database
    init_db()
    session = get_session()

    # Scrape each roaster
    for roaster_name, url in ROASTER_URLS.items():
        print(f"Scraping {roaster_name} at {url}")
        try:
            # Get products and variants
            products = scraper.get_products(url)
            if not products.empty:
                variants = scraper.get_variants(products)
                
                # Store in database
                store_data(roaster_name, url, products, variants, session)
                print(f"Stored {len(products)} products and {len(variants)} variants for {roaster_name}")
            else:
                print(f"No products found for {roaster_name}")
        except Exception as e:
            print(f"Error scraping {roaster_name}: {str(e)}")
            continue

    # Commit all changes
    session.commit()
    print("All data has been stored in the database")

if __name__ == "__main__":
    main()
