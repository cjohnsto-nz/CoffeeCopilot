from typing import Dict, List, Optional
import json
import os
from openai import AzureOpenAI
from bs4 import BeautifulSoup
import yaml
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

class AICoffeeExtractor:
    def __init__(self):
        """Initialize the AI-based coffee data extractor"""
        # Load environment variables
        load_dotenv()
        
        # Load config
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            self.ai_config = config['ai']

        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.temperature = self.ai_config['azure']['temperature']
        self.max_retries = self.ai_config['max_retries']

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content and extract text"""
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, 'html.parser')
        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.decompose()
        # Get text while preserving some structure
        lines = []
        for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
            text = element.get_text(strip=True)
            if text:
                lines.append(text)
        return '\n'.join(lines)

    def _get_empty_result(self) -> Dict:
        """Return an empty result structure"""
        return {
            "is_single_origin": None,
            "origin": {
                "country": None,
                "region": None
            },
            "processing_method": None,
            "varietals": [],
            "altitude": None,
            "farm": None,
            "producer": None,
            "tasting_notes": {
                "fruits": [],
                "sweets": [],
                "florals": [],
                "spices": [],
                "others": []
            },
            "confidence_score": 0.0
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def extract_coffee_data(self, body_html: str, tags: List[str] = None, scraped_html: str = None, parent_title: str = None) -> Dict:
        """Extract structured coffee data from product description using Azure OpenAI"""
        try:
            # Clean and combine text
            text = []
            
            # Add parent title if available
            if parent_title:
                text.append("=== PRODUCT TITLE ===")
                text.append(parent_title)
            
            # Add original body_html
            if body_html:
                text.append("\n=== SHOPIFY PRODUCT DESCRIPTION ===")
                text.append(self._clean_html(body_html))
            
            # Add scraped HTML content if available
            if scraped_html:
                text.append("\n=== SCRAPED PRODUCT PAGE CONTENT ===")
                text.append(self._clean_html(scraped_html))
            
            # Add tags
            if tags:
                text.append("\n=== PRODUCT TAGS ===")
                text.append(f"Tags: {', '.join(tags)}")
            
            # Create prompt
            prompt = f"""Extract coffee product details from this text. Pay special attention to the PRODUCT TITLE for determining if this is a blend or single origin coffee.

            Return a JSON object with these fields:
            - is_single_origin: true/false/null (if unclear)
              - true if it's from one specific farm, producer, or region
              - false if it contains the word "blend" or mentions multiple origins
              - null if unclear
            - origin: {{"country": string/null, "region": string/null}}
            - processing_method: string/null
            - varietals: list of strings
            - altitude: string/null (include units)
            - farm: string/null
            - producer: string/null
            - tasting_notes: {{
                "fruits": list of strings,
                "sweets": list of strings,
                "florals": list of strings,
                "spices": list of strings,
                "others": list of strings
            }}
            - confidence_score: float (0-1)

            Also estimate a recommended resting period in days based on these guidelines:
            - Natural/honey process: 10-14 days
            - Washed process: 7-10 days
            - Darker roasts: Add 2-3 days
            - African varietals (SL28, SL34, Ruiru 11, Batian): Add 1-2 days
            - Dense beans (high altitude >1600m): Add 1-2 days
            Return this as resting_period_days in the JSON.

            Text:
            {'\n'.join(text)}"""

            # Get completion from Azure OpenAI
            completion = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"}
            )

            # Parse response
            response = json.loads(completion.choices[0].message.content)
            
            # Ensure all fields exist
            response.setdefault('is_single_origin', None)
            response.setdefault('origin', {'country': None, 'region': None})
            response.setdefault('processing_method', None)
            response.setdefault('varietals', [])
            response.setdefault('altitude', None)
            response.setdefault('farm', None)
            response.setdefault('producer', None)
            response.setdefault('resting_period_days', None)
            response.setdefault('tasting_notes', {
                'fruits': [],
                'sweets': [],
                'florals': [],
                'spices': [],
                'others': []
            })
            response.setdefault('confidence_score', 0.0)
            
            return response

        except Exception as e:
            print(f"Error extracting coffee data: {str(e)}")
            return self._get_empty_result()
