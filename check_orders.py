from order_manager import get_order_history
from datetime import datetime

orders = get_order_history()
print("Recent orders:")
for status, order_list in orders.items():
    if order_list:  # Skip empty lists (like discontinued if not included)
        print(f"\n{status.title()}:")
        for order in order_list:
            # Parse the date string if needed
            order_date = order['order_date']
            if isinstance(order_date, str):
                order_date = datetime.fromisoformat(order_date.replace('Z', '+00:00'))
            print(f"- {order_date.strftime('%Y-%m-%d')}: {order['product_title']} (${order['price_paid']:.2f})")
