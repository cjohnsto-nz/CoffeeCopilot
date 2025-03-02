from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, JSON, Table, text, Boolean
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

def init_db():
    """Initialize the database, creating all tables and views"""
    Base.metadata.create_all(engine)
    create_beans_view(engine)

def create_beans_view(engine):
    """Create a view for whole bean products"""
    drop_view_query = "DROP VIEW IF EXISTS whole_beans_view;"
    view_query = """
    CREATE VIEW whole_beans_view AS
    WITH RankedVariants AS (
        SELECT 
            v.*,
            p.url as product_url,
            p.body_html,
            p.tags,
            ed.origin_country,
            ed.origin_region,
            ed.processing_method,
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
                PARTITION BY v.parent_title, v.vendor 
                ORDER BY v.grams
            ) as rank
        FROM variants v
        JOIN products p ON v.product_id = p.id
        LEFT JOIN product_extended_details ed ON p.id = ed.product_id
        WHERE LOWER(v.option2) LIKE '%bean%'
        AND v.grams BETWEEN 200 AND 250
        AND LOWER(v.parent_title) NOT LIKE '%espresso%'
        AND LOWER(v.parent_title) NOT LIKE '%subscription%'
        AND LOWER(v.parent_title) NOT LIKE '%decaf%'
        AND v.vendor != 'AAZ B2B'
    )
    SELECT 
        rv.parent_title,
        rv.vendor,
        rv.title as variant_title,
        rv.price,
        rv.grams,
        rv.option1,
        rv.option2,
        rv.option3,
        rv.available,
        rv.sku,
        rv.product_url,
        rv.body_html,
        rv.tags,
        rv.origin_country,
        rv.origin_region,
        rv.processing_method,
        rv.varietals,
        rv.altitude,
        rv.farm,
        rv.producer,
        rv.tasting_notes,
        rv.extraction_confidence,
        rv.is_single_origin,
        rv.resting_period_days,
        rv.adjusted_resting_period_days,
        CASE 
            WHEN rv.is_single_origin = 1 THEN 'Single Origin'
            WHEN rv.is_single_origin = 0 THEN 'Blend'
            ELSE NULL
        END as coffee_type
    FROM RankedVariants rv
    WHERE rv.rank = 1;
    """
    with engine.connect() as conn:
        conn.execute(text(drop_view_query))
        conn.execute(text(view_query))

def get_session():
    """Get a new database session"""
    Session = sessionmaker(bind=engine)
    return Session()
