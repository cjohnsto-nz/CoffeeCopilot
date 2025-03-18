from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, JSON, Table, text, Boolean, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# Create engine with SQLite's native Unicode support
engine = create_engine('sqlite:///coffee_data.db', connect_args={'check_same_thread': False})
Base = declarative_base()

class Roaster(Base):
    __tablename__ = 'roasters'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    description = Column(String(200))  # Friendly name for display
    url = Column(String(500))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    products = relationship("Product", back_populates="roaster")

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    roaster_id = Column(Integer, ForeignKey('roasters.id'))
    title = Column(String(200))
    handle = Column(String(200))
    body_html = Column(String)  # SQLite handles Unicode natively
    published_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    vendor = Column(String)
    product_type = Column(String)
    tags = Column(String)
    url = Column(String(500))
    parent_title = Column(String(200))
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    roaster = relationship("Roaster", back_populates="products")
    options = relationship("ProductOption", back_populates="product", cascade="all, delete-orphan")
    variants = relationship("Variant", back_populates="product", cascade="all, delete-orphan")
    images = relationship("ProductImage", back_populates="product", cascade="all, delete-orphan")
    extended_details = relationship("ProductExtendedDetails", back_populates="product", uselist=False, cascade="all, delete-orphan")

class ProductOption(Base):
    __tablename__ = 'product_options'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    name = Column(String(100))
    position = Column(Integer)
    values = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    product = relationship("Product", back_populates="options")

class Variant(Base):
    __tablename__ = 'variants'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    title = Column(String(200))
    available = Column(Integer, default=0)
    compare_at_price = Column(Float)
    featured_image = Column(String(500))
    grams = Column(Integer)
    option1 = Column(String(100))
    option2 = Column(String(100))
    option3 = Column(String(100))
    position = Column(Integer)
    price = Column(Float)
    requires_shipping = Column(Integer, default=1)
    sku = Column(String(100))
    taxable = Column(Integer, default=1)
    parent_title = Column(String(200))
    vendor = Column(String(200))
    weight = Column(Float)
    weight_unit = Column(String(10))
    barcode = Column(String(100))
    inventory_quantity = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    last_updated = Column(DateTime, default=datetime.now)
    
    # Relationships
    product = relationship("Product", back_populates="variants")

class ProductImage(Base):
    __tablename__ = 'product_images'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    position = Column(Integer)
    src = Column(String(500))
    width = Column(Integer)
    height = Column(Integer)
    alt = Column(String(200))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    product = relationship("Product", back_populates="images")

class ProductExtendedDetails(Base):
    __tablename__ = 'product_extended_details'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), unique=True)
    is_single_origin = Column(Boolean)
    origin_country = Column(String(100))
    origin_region = Column(String(100))
    roast_level = Column(String(50))
    processing_method = Column(String(100))
    varietals = Column(String(500))  # Comma-separated list
    altitude = Column(String(100))
    farm = Column(String(200))
    producer = Column(String(200))
    tasting_notes = Column(JSON)
    resting_period_days = Column(Integer)
    extraction_confidence = Column(Float)
    last_updated = Column(DateTime, default=datetime.now)
    
    # Relationships
    product = relationship("Product", back_populates="extended_details")

class OrderHistory(Base):
    __tablename__ = 'order_history'
    
    id = Column(Integer, primary_key=True)
    # Reference to the original product and variant
    product_id = Column(Integer, ForeignKey('products.id'))  # Can be null if product is deleted
    variant_id = Column(Integer, ForeignKey('variants.id'))  # Can be null if variant is deleted
    
    # Order details
    order_date = Column(DateTime, default=datetime.now)
    quantity = Column(Integer, nullable=False)
    price_paid = Column(Float, nullable=False)
    notes = Column(String)  # For any personal notes about the order
    
    # Key product details at time of purchase (preserved even if product is deleted)
    roaster_name = Column(String(100), nullable=False)
    product_title = Column(String(200), nullable=False)
    product_url = Column(String(500))
    option1 = Column(String(100))  # Usually size/weight
    option2 = Column(String(100))  # Usually bean type (Whole Bean)
    option3 = Column(String(100))  # Additional options
    
    # Coffee attributes at time of purchase
    is_single_origin = Column(Boolean)
    origin_country = Column(String(100))
    origin_region = Column(String(100))
    roast_level = Column(String(50))
    processing_method = Column(String(100))
    varietals = Column(String(500))
    altitude = Column(String(100))
    farm = Column(String(200))
    producer = Column(String(200))
    tasting_notes = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    product = relationship("Product")
    variant = relationship("Variant")

def init_db():
    """Initialize the database, creating all tables"""
    inspector = inspect(engine)
    
    # Get existing tables
    existing_tables = inspector.get_table_names()
    
    # If tables exist, alter them to add new columns
    if 'roasters' in existing_tables:
        with engine.connect() as conn:
            # Check if description column exists
            columns = [col['name'] for col in inspector.get_columns('roasters')]
            if 'description' not in columns:
                conn.execute(text("ALTER TABLE roasters ADD COLUMN description VARCHAR(200)"))
                conn.commit()
    
    # Create or update all tables
    Base.metadata.create_all(engine)
    
    # Create or update views
    create_beans_view(engine)
    create_order_history_view(engine)
    create_available_options_view(engine)

def create_beans_view(engine):
    """Create a view for whole bean products"""
    drop_sql = "DROP VIEW IF EXISTS whole_beans_view"
    view_sql = """
    CREATE VIEW whole_beans_view AS
    WITH ranked_variants AS (
        SELECT 
            v.parent_title,
            r.name as roaster_name,
            v.price,
            v.option1,
            v.option2,
            v.option3,
            v.available,
            p.url as product_url,
            p.id as product_id,
            v.id as variant_id,
            CASE 
                WHEN LOWER(v.parent_title) LIKE '%blend%' THEN 'Blend'
                WHEN ed.is_single_origin = 1 THEN 'Single Origin'
                WHEN ed.is_single_origin = 0 THEN 'Blend'
                WHEN LOWER(v.parent_title) LIKE '% and %' OR LOWER(v.parent_title) LIKE '% & %' THEN 'Blend'
                ELSE 'Unknown'
            END as coffee_type,
            ed.origin_country,
            ed.origin_region,
            ed.processing_method,
            ed.roast_level,
            ed.varietals,
            ed.altitude,
            ed.farm,
            ed.producer,
            ed.tasting_notes,
            ed.extraction_confidence,
            ed.is_single_origin,
            ed.resting_period_days,
            CASE 
                WHEN ed.resting_period_days IS NOT NULL THEN ed.resting_period_days * 2
                ELSE NULL
            END as adjusted_resting_period_days,
            ROW_NUMBER() OVER (
                PARTITION BY v.parent_title, r.name 
                ORDER BY v.grams
            ) as rank
        FROM variants v
        JOIN products p ON v.product_id = p.id
        JOIN roasters r ON p.roaster_id = r.id
        LEFT JOIN product_extended_details ed ON p.id = ed.product_id
        WHERE LOWER(v.option2) LIKE '%bean%'
        AND (v.option1 like '%250%' OR v.option1 like '%200g%')
        AND LOWER(v.parent_title) NOT LIKE '%espresso%'
        AND LOWER(v.parent_title) NOT LIKE '%subscription%'
        AND LOWER(v.parent_title) NOT LIKE '%decaf%'
        AND LOWER(v.parent_title) NOT LIKE '%voucher%'
        AND v.vendor != 'AAZ B2B'
        AND (v.option3 != 'READY TO DRINK' OR v.option3 IS NULL)
        AND v.available = 1
    )
    SELECT 
        rv.parent_title,
        rv.roaster_name,
        rv.price,
        rv.option1,
        rv.option2,
        rv.option3,
        rv.available,
        rv.product_url,
        rv.product_id,
        rv.variant_id,
        rv.coffee_type,
        rv.origin_country,
        rv.origin_region,
        rv.processing_method,
        rv.roast_level,
        rv.varietals,
        rv.altitude,
        rv.farm,
        rv.producer,
        rv.tasting_notes,
        rv.extraction_confidence,
        rv.is_single_origin,
        rv.resting_period_days,
        rv.adjusted_resting_period_days,
        rv.rank
    FROM ranked_variants rv
    WHERE rv.rank = 1
    """
    with engine.connect() as conn:
        conn.execute(text(drop_sql))
        conn.execute(text(view_sql))
        conn.commit()

def create_order_history_view(engine):
    """Create a view combining order history with current product details"""
    query = text("""
    CREATE VIEW order_history_view AS
    SELECT 
        oh.*,
        r.description as roaster_display_name,
        wb.parent_title,
        wb.processing_method,
        wb.origin_country,
        wb.origin_region,
        wb.roast_level,
        wb.tasting_notes
    FROM order_history oh
    JOIN whole_beans_view wb ON oh.product_id = wb.product_id
    JOIN roasters r ON wb.roaster_name = r.name
    ORDER BY oh.order_date DESC
    """)
    
    with engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS order_history_view"))
        conn.execute(query)
        conn.commit()

def create_available_options_view(engine):
    """Create a view of available coffees that haven't been ordered yet, excluding blends"""
    drop_query = text("DROP VIEW IF EXISTS available_options_view")
    
    create_query = text("""
    CREATE VIEW available_options_view AS
    SELECT DISTINCT 
        wb.*,
        p.url
    FROM whole_beans_view wb
    JOIN products p ON wb.product_id = p.id
    WHERE NOT EXISTS (
        SELECT 1 
        FROM order_history oh 
        WHERE oh.product_id = wb.product_id
        AND oh.variant_id = wb.variant_id
    )
    AND wb.coffee_type = 'Single Origin'
    ORDER BY wb.roaster_name, wb.parent_title
    """)
    
    with engine.connect() as conn:
        conn.execute(drop_query)
        conn.execute(create_query)
        conn.commit()

def get_session():
    """Get a new database session"""
    Session = sessionmaker(bind=engine)
    return Session()
