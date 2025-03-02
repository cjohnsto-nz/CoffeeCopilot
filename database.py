from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class Roaster(Base):
    __tablename__ = 'roasters'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    products = relationship("Product", back_populates="roaster")

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    roaster_id = Column(Integer, ForeignKey('roasters.id'))
    title = Column(String)
    handle = Column(String)
    body_html = Column(String)
    published_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    vendor = Column(String)
    product_type = Column(String)
    tags = Column(String)  # Store as comma-separated string
    url = Column(String)
    price = Column(Float)
    description = Column(String)
    last_updated = Column(DateTime)
    
    roaster = relationship("Roaster", back_populates="products")
    variants = relationship("Variant", back_populates="product")
    options = relationship("ProductOption", back_populates="product")
    images = relationship("ProductImage", back_populates="product")

class Variant(Base):
    __tablename__ = 'variants'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
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
    requires_shipping = Column(Integer)  # Store as 0/1 for boolean
    sku = Column(String)
    taxable = Column(Integer)  # Store as 0/1 for boolean
    updated_at = Column(DateTime)
    parent_title = Column(String)
    vendor = Column(String)
    weight = Column(String)
    last_updated = Column(DateTime)
    
    product = relationship("Product", back_populates="variants")

class ProductOption(Base):
    __tablename__ = 'product_options'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    name = Column(String)
    values = Column(String)  # Store as comma-separated string
    
    product = relationship("Product", back_populates="options")

class ProductImage(Base):
    __tablename__ = 'product_images'
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    url = Column(String)
    position = Column(Integer)
    
    product = relationship("Product", back_populates="images")

def create_beans_view(engine):
    drop_view_query = "DROP VIEW IF EXISTS whole_beans_view;"
    view_query = """
    CREATE VIEW whole_beans_view AS
    WITH RankedVariants AS (
        SELECT 
            v.*,
            p.url as product_url,
            ROW_NUMBER() OVER (
                PARTITION BY v.parent_title, v.vendor 
                ORDER BY v.grams
            ) as rank
        FROM variants v
        JOIN products p ON v.product_id = p.id
        WHERE LOWER(v.option2) LIKE '%bean%'
        AND v.grams BETWEEN 200 AND 250
        AND LOWER(v.parent_title) NOT LIKE '%espresso%'
        AND LOWER(v.parent_title) NOT LIKE '%subscription%'
        AND LOWER(v.parent_title) NOT LIKE '%decaf%'
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
        rv.product_url
    FROM RankedVariants rv
    WHERE rv.rank = 1;
    """
    with engine.connect() as conn:
        conn.execute(text(drop_view_query))
        conn.execute(text(view_query))
        conn.commit()

def init_db():
    engine = create_engine('sqlite:///coffee_data.db')
    Base.metadata.create_all(engine)
    create_beans_view(engine)
    return engine

def get_session():
    engine = create_engine('sqlite:///coffee_data.db')
    Session = sessionmaker(bind=engine)
    return Session()

def get_beans():
    session = get_session()
    result = session.execute(text("SELECT * FROM whole_beans_view"))
    columns = result.keys()
    beans = [dict(zip(columns, row)) for row in result]
    session.close()
    return beans
