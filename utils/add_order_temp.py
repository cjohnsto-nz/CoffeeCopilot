from datetime import datetime
from coffee_copilot.order_manager import find_product_by_name, add_order

def add_coffee_order(name, date):
    product = find_product_by_name(name)
    if product:
        order = add_order(
            product_id=product['product_id'],
            variant_id=product['variant_id'],
            quantity=1,
            price_paid=product['price'],
            order_date=date
        )
        print(f'Added order for {product["parent_title"]} at ${product["price"]:.2f}')
    else:
        print(f'Could not find product matching "{name}"')

# Add the order
add_coffee_order(
    'Colombia Sebastian Gomez Castillo Washed',
    datetime(2025, 3, 13)
)
