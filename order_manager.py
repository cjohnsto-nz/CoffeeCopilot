from datetime import datetime
from typing import Optional, Dict, Any
from database import get_session, Product, Variant, ProductExtendedDetails, OrderHistory
from sqlalchemy import text

def add_order(
    product_id: int,
    variant_id: int,
    quantity: int,
    price_paid: float,
    order_date: Optional[datetime] = None,
    notes: Optional[str] = None,
    session = None
) -> OrderHistory:
    """
    Add a new order to the order history.
    
    Args:
        product_id: ID of the product being ordered
        variant_id: ID of the specific variant being ordered
        quantity: Number of units ordered
        price_paid: Total price paid for the order
        order_date: Date of purchase (defaults to current time if not specified)
        notes: Optional notes about the order
        session: Optional database session (will create one if not provided)
    
    Returns:
        The created OrderHistory record
        
    Raises:
        ValueError: If the product or variant is not found
    """
    session_created = False
    if session is None:
        session = get_session()
        session_created = True
        
    try:
        # Get the product and its relationships
        product = session.query(Product).filter(Product.id == product_id).first()
        if not product:
            raise ValueError(f"Product with ID {product_id} not found")
            
        variant = session.query(Variant).filter(Variant.id == variant_id).first()
        if not variant:
            raise ValueError(f"Variant with ID {variant_id} not found")
            
        extended_details = product.extended_details
        
        # Create order history entry
        order = OrderHistory(
            product_id=product.id,
            variant_id=variant.id,
            quantity=quantity,
            price_paid=price_paid,
            notes=notes,
            order_date=order_date or datetime.now(),
            
            # Store product details at time of purchase
            roaster_name=product.roaster.name,
            product_title=product.parent_title or product.title,
            product_url=product.url,
            option1=variant.option1,
            option2=variant.option2,
            option3=variant.option3,
            
            # Store coffee attributes at time of purchase
            is_single_origin=extended_details.is_single_origin if extended_details else None,
            origin_country=extended_details.origin_country if extended_details else None,
            origin_region=extended_details.origin_region if extended_details else None,
            roast_level=extended_details.roast_level if extended_details else None,
            processing_method=extended_details.processing_method if extended_details else None,
            varietals=extended_details.varietals if extended_details else None,
            altitude=extended_details.altitude if extended_details else None,
            farm=extended_details.farm if extended_details else None,
            producer=extended_details.producer if extended_details else None,
            tasting_notes=extended_details.tasting_notes if extended_details else None
        )
        
        session.add(order)
        session.commit()
        return order
        
    except Exception as e:
        session.rollback()
        raise e
    finally:
        if session_created:
            session.close()

def get_order_history(include_discontinued: bool = True, session = None) -> Dict[str, Any]:
    """
    Get the order history with current product status.
    
    Args:
        include_discontinued: If True, include discontinued products
        session: Optional database session (will create one if not provided)
    
    Returns:
        Dictionary containing lists of orders grouped by status:
        {
            "available": [...],
            "out_of_stock": [...],
            "discontinued": [...]  # Only if include_discontinued is True
        }
    """
    session_created = False
    if session is None:
        session = get_session()
        session_created = True
        
    try:
        # Build the query
        query = """
        SELECT *
        FROM order_history_view
        WHERE 1=1
        """
        if not include_discontinued:
            query += " AND product_status != 'Discontinued'"
        query += " ORDER BY order_date DESC"
        
        # Execute with text() wrapper
        results = session.execute(text(query)).fetchall()
        
        # Group orders by status
        orders = {
            "available": [],
            "out_of_stock": [],
            "discontinued": [] if include_discontinued else None
        }
        
        for row in results:
            # Convert SQLAlchemy Row to dict
            row_dict = {key: getattr(row, key) for key in row._fields}
            status = row_dict["product_status"].lower().replace(" ", "_")
            if status in orders and orders[status] is not None:
                orders[status].append(row_dict)
        
        return orders
        
    finally:
        if session_created:
            session.close()

def get_order_details(order_id: int, session = None) -> Dict[str, Any]:
    """
    Get detailed information about a specific order.
    
    Args:
        order_id: ID of the order to retrieve
        session: Optional database session (will create one if not provided)
    
    Returns:
        Dictionary containing order details including current product status
        
    Raises:
        ValueError: If the order is not found
    """
    session_created = False
    if session is None:
        session = get_session()
        session_created = True
        
    try:
        # Query the order_history_view for this specific order
        query = text("""
        SELECT *
        FROM order_history_view
        WHERE id = :order_id
        """)
        
        result = session.execute(query, {"order_id": order_id}).first()
        if not result:
            raise ValueError(f"Order with ID {order_id} not found")
            
        # Convert SQLAlchemy Row to dict
        return {key: getattr(result, key) for key in result._fields}
        
    finally:
        if session_created:
            session.close()

def find_product_by_name(name: str, session = None) -> Dict[str, Any]:
    """
    Find a product in the whole_beans_view by its name.
    Performs a case-insensitive partial match.
    
    Args:
        name: Name of the product to find
        session: Optional database session (will create one if not provided)
        
    Returns:
        Dictionary with product details if found, None if not found
    """
    session_created = False
    if session is None:
        session = get_session()
        session_created = True
        
    try:
        # First get the product from whole_beans_view
        query = text("""
        SELECT 
            product_id,
            parent_title,
            roaster_name,
            price
        FROM whole_beans_view
        WHERE LOWER(parent_title) LIKE :name
        """)
        
        result = session.execute(query, {"name": f"%{name.lower()}%"}).fetchall()
        
        if not result:
            return None
            
        # If multiple matches, print them all and return None
        if len(result) > 1:
            print(f"Multiple matches found for '{name}':")
            for r in result:
                print(f"- {r.parent_title} ({r.roaster_name})")
            return None
            
        # Convert SQLAlchemy Row to dict
        row = result[0]
        product = {
            "product_id": row.product_id,
            "parent_title": row.parent_title,
            "roaster_name": row.roaster_name,
            "price": row.price
        }
        
        # Get the first available variant for this product
        variant_query = text("""
        SELECT id 
        FROM variants 
        WHERE product_id = :product_id 
        LIMIT 1
        """)
        
        variant = session.execute(variant_query, {"product_id": product["product_id"]}).first()
        if not variant:
            print(f"No variants found for product {product['parent_title']}")
            return None
            
        product["variant_id"] = variant.id
        return product
        
    finally:
        if session_created:
            session.close()

def add_coffee_order(coffee_name: str, order_date: datetime):
    """Add a coffee order to the order history"""
    session = get_session()
    
    try:
        # Extract product and variant IDs from coffee name
        if coffee_name.startswith('['):
            try:
                # Extract IDs and title
                id_end = coffee_name.index(']')
                ids = coffee_name[1:id_end].split(',')
                product_id = int(ids[0])
                variant_id = int(ids[1])
                title = coffee_name[id_end + 2:]  # Skip "] " to get title
                
                # First try exact ID match
                query = text("""
                    SELECT 
                        wb.product_id,
                        wb.variant_id,
                        r.description as roaster_name,
                        wb.parent_title,
                        wb.url as product_url,
                        v.option1,
                        v.option2,
                        v.option3,
                        wb.price as price_paid,
                        ed.is_single_origin,
                        wb.origin_country,
                        ed.origin_region,
                        ed.roast_level,
                        wb.processing_method,
                        ed.varietals,
                        ed.altitude,
                        ed.farm,
                        ed.producer,
                        wb.tasting_notes
                    FROM available_options_view wb
                    JOIN roasters r ON wb.roaster_name = r.name
                    JOIN variants v ON wb.product_id = v.product_id AND wb.variant_id = v.id
                    LEFT JOIN product_extended_details ed ON wb.product_id = ed.product_id
                    WHERE wb.product_id = :product_id
                    AND wb.variant_id = :variant_id
                    LIMIT 1
                """)
                result = session.execute(query, {
                    "product_id": product_id,
                    "variant_id": variant_id
                }).fetchone()
                
                # If ID match fails, try title match
                if not result:
                    query = text("""
                        SELECT 
                            wb.product_id,
                            wb.variant_id,
                            r.description as roaster_name,
                            wb.parent_title,
                            wb.url as product_url,
                            v.option1,
                            v.option2,
                            v.option3,
                            wb.price as price_paid,
                            ed.is_single_origin,
                            wb.origin_country,
                            ed.origin_region,
                            ed.roast_level,
                            wb.processing_method,
                            ed.varietals,
                            ed.altitude,
                            ed.farm,
                            ed.producer,
                            wb.tasting_notes
                        FROM available_options_view wb
                        JOIN roasters r ON wb.roaster_name = r.name
                        JOIN variants v ON wb.product_id = v.product_id AND wb.variant_id = v.id
                        LEFT JOIN product_extended_details ed ON wb.product_id = ed.product_id
                        WHERE wb.parent_title = :title
                        AND v.option2 = 'Whole Bean'
                        LIMIT 1
                    """)
                    result = session.execute(query, {"title": title}).fetchone()
            except (ValueError, IndexError):
                result = None
        else:
            # Legacy support for coffee names without IDs
            query = text("""
                SELECT 
                    wb.product_id,
                    wb.variant_id,
                    r.description as roaster_name,
                    wb.parent_title,
                    wb.url as product_url,
                    v.option1,
                    v.option2,
                    v.option3,
                    wb.price as price_paid,
                    ed.is_single_origin,
                    wb.origin_country,
                    ed.origin_region,
                    ed.roast_level,
                    wb.processing_method,
                    ed.varietals,
                    ed.altitude,
                    ed.farm,
                    ed.producer,
                    wb.tasting_notes
                FROM available_options_view wb
                JOIN roasters r ON wb.roaster_name = r.name
                JOIN variants v ON wb.product_id = v.product_id AND wb.variant_id = v.id
                LEFT JOIN product_extended_details ed ON wb.product_id = ed.product_id
                WHERE wb.parent_title = :title
                AND v.option2 = 'Whole Bean'
                LIMIT 1
            """)
            result = session.execute(query, {"title": coffee_name}).fetchone()
        
        if not result:
            raise ValueError(f"Could not find product matching \"{coffee_name}\"")
        
        # Create new order history entry
        order = OrderHistory(
            product_id=result.product_id,
            variant_id=result.variant_id,
            order_date=order_date,
            quantity=1,
            price_paid=result.price_paid,
            roaster_name=result.roaster_name,
            product_title=result.parent_title,
            product_url=result.product_url,
            option1=result.option1,
            option2=result.option2,
            option3=result.option3,
            is_single_origin=result.is_single_origin,
            origin_country=result.origin_country,
            origin_region=result.origin_region,
            roast_level=result.roast_level,
            processing_method=result.processing_method,
            varietals=result.varietals,
            altitude=result.altitude,
            farm=result.farm,
            producer=result.producer,
            tasting_notes=result.tasting_notes
        )
        
        session.add(order)
        session.commit()
        print(f"\nAdded {result.roaster_name} - {result.parent_title} to order history")
        
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
