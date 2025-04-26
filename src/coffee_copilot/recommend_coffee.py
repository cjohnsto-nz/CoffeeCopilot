import os
import json
import sys
from datetime import datetime
from dotenv import load_dotenv
from coffee_copilot.database import get_session
from sqlalchemy import text
from openai import AzureOpenAI
import yaml
import logging

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
        self.prompt_log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs', 'prompts', 'recommendations')
        os.makedirs(self.prompt_log_dir, exist_ok=True)
        
        # Initialize conversation history
        self.conversation_history = [
            {"role": "system", "content": "You are a coffee recommendation assistant that helps users discover new and interesting coffees based on their order history and preferences."}
        ]
        
        # Track rejected recommendations
        self.rejected_recommendations = []
    
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
                oh.product_title as parent_title,
                r.description as roaster_name,
                oh.processing_method,
                oh.origin_country,
                json(oh.tasting_notes) as tasting_notes,
                oh.price_paid as price,
                oh.product_url as url
            FROM order_history oh
            LEFT JOIN roasters r ON oh.roaster_name = r.name OR oh.roaster_name = r.description
            ORDER BY oh.order_date DESC
        """)
        results = session.execute(query)
        rows = []
        for row in results:
            row_dict = dict(row._mapping)
            if row_dict['tasting_notes']:
                try:
                    # Handle case where tasting_notes is already a JSON string
                    if isinstance(row_dict['tasting_notes'], str):
                        row_dict['tasting_notes'] = json.loads(row_dict['tasting_notes'])
                except (json.JSONDecodeError, TypeError):
                    # If tasting_notes is not valid JSON
                    row_dict['tasting_notes'] = {}
            else:
                row_dict['tasting_notes'] = {}
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

    def get_recommendation(self, rejection_reason=None):
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
        
        for order in history:
            roaster = order.get('roaster_name', 'Unknown')
            origin = order.get('origin_country', 'Unknown')
            process = order.get('processing_method', 'Unknown')
            product_id = order.get('product_id')
            
            roaster_counts[roaster] = roaster_counts.get(roaster, 0) + 1
            origin_counts[origin] = origin_counts.get(origin, 0) + 1
            process_counts[process] = process_counts.get(process, 0) + 1
            
            if product_id:
                ordered_product_ids.add(product_id)
        
        # Format data for the prompt
        history_formatted = "\n".join([f"{i+1}. {self.format_coffee_data(coffee)}" for i, coffee in enumerate(history[:10])])
        options_formatted = "\n".join([f"{i+1}. [{option['product_id']},{option['variant_id']}] {option['roaster_name']} - {option['parent_title']} | {option['origin_country'] or 'Unknown origin'} | {option['processing_method'] or 'Unknown process'} | ${option['price']:.2f} | {option['url']}" for i, option in enumerate(options)])
        
        # Include rejected recommendations and reasons if available
        rejected_info = ""
        if self.rejected_recommendations:
            rejected_info = "\nPreviously rejected recommendations:\n"
            for i, (coffee, reason) in enumerate(self.rejected_recommendations):
                rejected_info += f"{i+1}. {coffee}" + (f" - Reason: {reason}" if reason else "") + "\n"
        
        # Create prompt
        prompt = f"""I need a coffee recommendation based on my order history and available options.

Order History (most recent first):
{history_formatted}

Available options:
{"="*80}
{options_formatted}
{rejected_info}

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
        
        # Add rejection reason to conversation history if provided
        if rejection_reason:
            self.conversation_history.append({"role": "user", "content": f"I didn't like the previous recommendation because: {rejection_reason}"})
        
        # Add the current prompt to conversation history
        self.conversation_history.append({"role": "user", "content": prompt})
        
        # Save prompt for debugging (silently)
        context = f"Order History: {len(history)} orders\nAvailable Options: {len(options)} coffees\nCurrent Month Spend: ${spending['current_month']:.2f}\nRemaining Budget: ${remaining_budget:.2f}"
        self._dump_prompt(prompt, context)
        
        # Get recommendation from GPT-4 using conversation history
        completion = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=self.conversation_history,
            temperature=0.7,
            max_tokens=500
        )
        
        # Add the response to conversation history
        response_content = completion.choices[0].message.content
        self.conversation_history.append({"role": "assistant", "content": response_content})
        
        return response_content

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
            from coffee_copilot.order_manager import add_coffee_order
            add_coffee_order(coffee_name, datetime.now())
            print(f"\nAdded {coffee_name} to order history")
        else:
            print("\nNo input received. Exiting without adding to order history.")
            
    except Exception as e:
        print(f"Error getting recommendation: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
