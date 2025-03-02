from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, JSON, Table, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

Base = declarative_base()

class Roaster(Base):
    __tablename__ = 'roasters'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    url = Column(String, unique=True)
    products = relationship("Product", back_populates="roaster")
    last_updated = Column(DateTime, default=datetime.now)

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    roaster_id = Column(Integer, ForeignKey('roasters.id'))
    roaster = relationship("Roaster", back_populates="products")
    
    # Basic product info
    title = Column(String)
    handle = Column(String)
    body_html = Column(String)
    published_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    vendor = Column(String)
    product_type = Column(String)
    tags = Column(String)
    url = Column(String)
    
    # Relationships
    options = relationship("ProductOption", back_populates="product", cascade="all, delete-orphan")
    images = relationship("ProductImage", back_populates="product", cascade="all, delete-orphan")
    variants = relationship("Variant", back_populates="product", cascade="all, delete-orphan")
    extended_details = relationship("ProductExtendedDetails", back_populates="product", uselist=False, cascade="all, delete-orphan")
    
    last_updated = Column(DateTime, default=datetime.now)

class ProductExtendedDetails(Base):
    __tablename__ = 'product_extended_details'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), unique=True)
    product = relationship("Product", back_populates="extended_details")
    
    # AI-extracted coffee info
    is_single_origin = Column(Integer)  # 1 for single origin, 0 for blend, NULL if unknown
    origin_country = Column(String)
    origin_region = Column(String)
    processing_method = Column(String)
    varietals = Column(String)  # Comma-separated list
    altitude = Column(String)  # Using string to handle ranges and units
    farm = Column(String)
    producer = Column(String)
    tasting_notes = Column(JSON)  # Structured as {"fruits": [], "sweets": [], etc.}
    resting_period_days = Column(Integer)  # Recommended resting period in days
    extraction_confidence = Column(Float)
    last_updated = Column(DateTime, default=datetime.now)

class ProductOption(Base):
    __tablename__ = 'product_options'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    product = relationship("Product", back_populates="options")
    name = Column(String)
    values = Column(String)  # Comma-separated list
    last_updated = Column(DateTime, default=datetime.now)

class ProductImage(Base):
    __tablename__ = 'product_images'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    product = relationship("Product", back_populates="images")
    url = Column(String)
    position = Column(Integer)
    last_updated = Column(DateTime, default=datetime.now)

class Variant(Base):
    __tablename__ = 'variants'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    product = relationship("Product", back_populates="variants")
    
    title = Column(String)
    available = Column(Integer)
    compare_at_price = Column(Float)
    created_at = Column(DateTime)
    featured_image = Column(String)
    grams = Column(Integer)
    option1 = Column(String)
    option2 = Column(String)
    option3 = Column(String)
    position = Column(Integer)
    price = Column(Float)
    requires_shipping = Column(Integer)
    sku = Column(String)
    taxable = Column(Integer)
    updated_at = Column(DateTime)
    parent_title = Column(String)
    vendor = Column(String)
    last_updated = Column(DateTime, default=datetime.now)

def create_beans_view(engine):
    """Create a view for whole bean products"""
    drop_view_query = "DROP VIEW IF EXISTS whole_beans_view;"
    view_query = """
    CREATE VIEW whole_beans_view AS
    WITH RankedVariants AS (
        SELECT 
            v.*,
            p.url as product_url,
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
        conn.commit()

def init_db():
    """Initialize the database, creating all tables and views"""
    engine = create_engine('sqlite:///coffee_data.db')
    Base.metadata.create_all(engine)
    create_beans_view(engine)

def get_session():
    """Get a new database session"""
    engine = create_engine('sqlite:///coffee_data.db')
    Session = sessionmaker(bind=engine)
    return Session()
