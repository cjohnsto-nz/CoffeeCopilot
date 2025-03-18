import os
from dotenv import load_dotenv
from datetime import datetime
from database import get_session
from sqlalchemy import text
from order_manager import add_coffee_order
import yaml
import json
from openai import AzureOpenAI
import sys

class CoffeeRecommender:
    def __init__(self):
        """Initialize the coffee recommendation system"""
        # Load environment variables
        load_dotenv()
        
        # Load config
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
            self.monthly_budget = self.config['preferences']['monthly_budget']
            self.budget_flexibility = self.config['preferences']['budget_flexibility']
        
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        
        # Create prompt logs directory
        self.prompt_log_dir = "prompt_logs/recommendations"
        os.makedirs(self.prompt_log_dir, exist_ok=True)
    
    def _dump_prompt(self, prompt: str, context: str):
        """Dump prompt to a file for debugging"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.prompt_log_dir}/{timestamp}_recommendation.txt"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write("=== CONTEXT ===\n")
            f.write(context + "\n\n")
            f.write("=== PROMPT ===\n")
            f.write(prompt)
            
    def get_order_history(self):
        session = get_session()
        query = text("""
            SELECT 
                oh.product_id,
                oh.variant_id,
                oh.order_date,
                wb.parent_title,
                r.description as roaster_name,
                wb.processing_method,
                wb.origin_country,
                json(wb.tasting_notes) as tasting_notes,
                oh.price_paid as price,
                p.url
            FROM order_history oh
            JOIN whole_beans_view wb ON oh.product_id = wb.product_id
            JOIN products p ON wb.product_id = p.id
            JOIN roasters r ON p.roaster_id = r.id
            ORDER BY oh.order_date DESC
        """)
        results = session.execute(query)
        rows = []
        for row in results:
            row_dict = dict(row._mapping)
            if row_dict['tasting_notes']:
                row_dict['tasting_notes'] = json.loads(row_dict['tasting_notes'])
            rows.append(row_dict)
        return rows

    def parse_date(self, date_str):
        """Parse date string into datetime object"""
        if isinstance(date_str, str):
            try:
                return datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
        return date_str

    def get_monthly_spend(self, history, year, month):
        """Calculate total spend for a specific month"""
        total = 0
        for order in history:
            order_date = self.parse_date(order['order_date'])
            if order_date.year == year and order_date.month == month:
                total += order['price']
        return total

    def get_spending_summary(self, history):
        """Generate a summary of recent spending"""
        now = datetime.now()
        current_month_spend = self.get_monthly_spend(history, now.year, now.month)
        last_month_spend = self.get_monthly_spend(history, now.year if now.month > 1 else now.year - 1, 
                                           now.month - 1 if now.month > 1 else 12)
        
        # Get last 3 months average (excluding current month)
        three_month_total = 0
        for i in range(1, 4):
            month = now.month - i
            year = now.year
            if month <= 0:
                month += 12
                year -= 1
            three_month_total += self.get_monthly_spend(history, year, month)
        
        return {
            'current_month': current_month_spend,
            'last_month': last_month_spend,
            'three_month_average': three_month_total / 3
        }

    def get_available_options(self):
        session = get_session()
        query = text("""
            SELECT 
                wb.product_id,
                wb.variant_id,
                wb.parent_title,
                r.description as roaster_name,
                wb.processing_method,
                wb.origin_country,
                json(wb.tasting_notes) as tasting_notes,
                wb.price,
                wb.url
            FROM available_options_view wb
            JOIN roasters r ON wb.roaster_name = r.name
            ORDER BY r.description
        """)
        
        results = session.execute(query)
        rows = []
        for row in results:
            row_dict = dict(row._mapping)
            if row_dict['tasting_notes']:
                row_dict['tasting_notes'] = json.loads(row_dict['tasting_notes'])
            rows.append(row_dict)
        return rows

    def format_coffee_data(self, coffee):
        """Format coffee data into a readable string"""
        parts = [
            f"[{coffee['product_id']},{coffee['variant_id']}] {coffee['roaster_name']} - {coffee['parent_title']}",
            f"Origin: {coffee['origin_country'] or 'Unknown'}"
        ]
        
        if coffee['processing_method']:
            parts.append(f"Process: {coffee['processing_method']}")
        if coffee['tasting_notes']:
            parts.append(f"Tasting notes: {coffee['tasting_notes']}")
        parts.append(f"Price: ${coffee['price']:.2f}")
        parts.append(f"URL: {coffee['url']}")
        
        return " | ".join(parts)

    def get_recommendation(self):
        # Get data
        history = self.get_order_history()
        options = self.get_available_options()
        spending = self.get_spending_summary(history)
        
        # Calculate remaining budget
        remaining_budget = self.monthly_budget - spending['current_month']
        max_price = self.monthly_budget * (1 + self.budget_flexibility)
        
        # Analyze current distribution
        roaster_counts = {}
        origin_counts = {}
        process_counts = {}
        ordered_product_ids = set()
        
        for coffee in history:
            ordered_product_ids.add(coffee['product_id'])
            roaster_counts[coffee['roaster_name']] = roaster_counts.get(coffee['roaster_name'], 0) + 1
            if coffee['origin_country']:
                origin_counts[coffee['origin_country']] = origin_counts.get(coffee['origin_country'], 0) + 1
            if coffee['processing_method']:
                process_counts[coffee['processing_method']] = process_counts.get(coffee['processing_method'], 0) + 1
        
        # Filter out ordered coffees from options
        available_options = [opt for opt in options if opt['product_id'] not in ordered_product_ids]
        
        # Format history and options
        history_formatted = "\n".join(self.format_coffee_data(coffee) for coffee in history[:5])
        options_formatted = "\n".join(self.format_coffee_data(coffee) for coffee in available_options)
        
        # Format the prompt
        prompt = f"""You are a coffee expert helping select the next coffee to try. Your goal is to help the user explore new and different coffee experiences.

Recent order history (from newest to oldest) - DO NOT recommend any coffees from this list:
{"="*80}
{history_formatted}

Available options to choose from - You MUST select from this list:
{"="*80}
{options_formatted}

Based on the order history and available options, recommend ONE coffee that would maximize variety in terms of roaster, origin, processing method, and tasting notes.

Current distribution in order history:
- Most frequent roasters: {', '.join(f'{k} ({v}x)' for k, v in sorted(roaster_counts.items(), key=lambda x: x[1], reverse=True)[:3])}
- Most frequent origins: {', '.join(f'{k} ({v}x)' for k, v in sorted(origin_counts.items(), key=lambda x: x[1], reverse=True)[:3])}
- Processing methods used: {', '.join(f'{k} ({v}x)' for k, v in sorted(process_counts.items(), key=lambda x: x[1], reverse=True))}

Budget Considerations:
- Monthly budget: ${self.monthly_budget:.2f}
- Remaining this month: ${remaining_budget:.2f}
- Can exceed monthly budget by up to {self.budget_flexibility*100}% for special coffees
- Maximum price for a special coffee: ${max_price:.2f}

CRITICAL INSTRUCTIONS:
1. You MUST select a coffee from the 'Available options' list above. DO NOT recommend anything from the order history.
2. First line: Format as "[product_id,variant_id] Roaster - Coffee" using the EXACT IDs and names from available options
3. Second line: Leave blank
4. Third line onwards: A single paragraph explaining why this coffee is interesting, focusing on how it differs from recent purchases
5. Final line: The EXACT URL from available options
6. Do not use markdown formatting
7. Do not include headings or sections
8. Do not mention price unless it's a special coffee that exceeds the monthly budget"""
        
        # Save prompt for debugging (silently)
        context = f"Order History: {len(history)} orders\nAvailable Options: {len(options)} coffees\nCurrent Month Spend: ${spending['current_month']:.2f}\nRemaining Budget: ${remaining_budget:.2f}"
        self._dump_prompt(prompt, context)
        
        # Get recommendation from GPT-4
        completion = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=500
        )
        
        return completion.choices[0].message.content

def main():
    try:
        recommender = CoffeeRecommender()
        
        # Get and display spending summary
        history = recommender.get_order_history()
        spending = recommender.get_spending_summary(history)
        
        print("\nSpending Summary:")
        print("-" * 50)
        print(f"Current Month: ${spending['current_month']:.2f}")
        print(f"Last Month: ${spending['last_month']:.2f}")
        print(f"3-Month Average: ${spending['three_month_average']:.2f}")
        
        # Display recent orders
        print("\nRecent Orders:")
        print("-" * 50)
        for coffee in history[:5]:
            order_date = recommender.parse_date(coffee['order_date']).strftime('%Y-%m-%d')
            print(f"{order_date}: {coffee['roaster_name']} - {coffee['parent_title']} (${coffee['price']:.2f})")
        
        # Get and display recommendation
        print("\nRecommended Coffee:")
        print("-" * 50)
        recommendation = recommender.get_recommendation()
        print(recommendation)
        
        # Ask if user wants to add to order history
        print("\nWould you like to add this to your order history? (yes/no): ")
        response = input().strip().lower()
        
        if response == 'yes':
            # Extract coffee name from recommendation
            coffee_name = recommendation.split('\n')[0].strip()
            add_coffee_order(coffee_name, datetime.now())
            print(f"\nAdded {coffee_name} to order history")
        else:
            print("\nNo input received. Exiting without adding to order history.")
            
    except Exception as e:
        print(f"Error getting recommendation: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
