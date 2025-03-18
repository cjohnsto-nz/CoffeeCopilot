from coffee_copilot.database import get_session
from sqlalchemy import text

def show_available_options():
    session = get_session()
    try:
        query = text("""
            SELECT roaster_name, parent_title, price, origin_country
            FROM available_options_view 
            ORDER BY roaster_name, parent_title
        """)
        
        results = session.execute(query)
        
        current_roaster = None
        for row in results:
            if row.roaster_name != current_roaster:
                print(f"\n{row.roaster_name}:")
                current_roaster = row.roaster_name
            
            origin = f" ({row.origin_country})" if row.origin_country else ""
            print(f"  - {row.parent_title}{origin} ${row.price:.2f}")
            
    finally:
        session.close()

if __name__ == "__main__":
    print("Available coffees you haven't tried yet:")
    show_available_options()
