from typing import Dict, List, Optional
import json
import os
from openai import AzureOpenAI
from bs4 import BeautifulSoup
import yaml
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from io import BytesIO
from PIL import Image
import base64
from datetime import datetime

class AICoffeeExtractor:
    def __init__(self):
        """Initialize the AI-based coffee data extractor"""
        # Load environment variables
        load_dotenv()
        
        # Load config
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
            self.ai_config = self.config['ai']
            self.image_config = self.config['image_processing']

        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.temperature = self.ai_config['azure']['temperature']
        self.max_retries = self.ai_config['max_retries']
        
        # Create prompt logs directory
        self.prompt_log_dir = "prompt_logs/extractions"
        os.makedirs(self.prompt_log_dir, exist_ok=True)
        
    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content and extract text"""
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.decompose()
            
        # Find all divs with 'desc' in their class name (catches description, desc, etc.)
        desc_divs = soup.find_all('div', class_=lambda x: x and 'desc' in x.lower())
        
        # Only use description divs, no fallback
        text = []
        for div in desc_divs:
            # Keep line breaks for structure
            lines = [line.strip() for line in div.get_text().split('\n') if line.strip()]
            text.extend(lines)
        return '\n'.join(text)

    def _get_empty_result(self) -> Dict:
        """Return an empty result structure"""
        return {
            "is_single_origin": None,
            "origin": {
                "country": None,
                "region": None
            },
            "roast_level": None,
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

    def _downsample_image(self, image_url: str) -> str:
        """Download, downsample maintaining aspect ratio, and convert image to base64"""
        try:
            # Get config values
            target_height = self.image_config['target_height']
            jpeg_quality = self.image_config['jpeg_quality']
            
            # Download image
            response = requests.get(image_url)
            response.raise_for_status()
            
            # Open image
            img = Image.open(BytesIO(response.content))
            img = img.convert('RGB')  # Convert to RGB to ensure JPEG compatibility
            
            # Calculate new width to maintain aspect ratio
            aspect_ratio = img.width / img.height
            new_width = int(target_height * aspect_ratio)
            
            # Resize image maintaining aspect ratio
            img = img.resize((new_width, target_height), Image.Resampling.LANCZOS)
            
            # Save to BytesIO in JPEG format with compression
            output = BytesIO()
            img.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
            output.seek(0)
            
            # Convert to base64
            base64_image = base64.b64encode(output.getvalue()).decode('utf-8')
            return f"data:image/jpeg;base64,{base64_image}"
            
        except Exception as e:
            print(f"Error processing image: {str(e)}")
            return None

    def _dump_prompt(self, prompt: str, title: str):
        """Dump prompt to a file for debugging"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        filename = f"{self.prompt_log_dir}/{timestamp}_{safe_title}.txt"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"Product: {title}\n")
            f.write("="*80 + "\n\n")
            f.write(prompt)
            
        print(f"Prompt dumped to: {filename}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=60, min=60, max=180))
    def extract_coffee_data(self, body_html: str, tags: List[str] = None, scraped_html: str = None, parent_title: str = None, image_url: str = None) -> Dict:
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
            
            # Create messages list for chat completion
            messages = []
            
            # If we have an image URL, process and add it
            if image_url:
                # Downsample image and convert to base64
                base64_image = self._downsample_image(image_url)
                
                if base64_image:
                    messages.append({
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": base64_image,
                                    "detail": "low"  # Use low detail since we've already downsampled
                                }
                            },
                            {
                                "type": "text",
                                "text": "Please analyze this coffee product image."
                            }
                        ]
                    })
                    
                    # Get image analysis from GPT-4V
                    image_completion = self.client.chat.completions.create(
                        model=self.deployment_name,
                        messages=messages,
                        temperature=0.0,
                        max_tokens=500
                    )
                    
                    # Add image analysis to text
                    text.append("\n=== IMAGE ANALYSIS ===")
                    text.append(image_completion.choices[0].message.content)
            
            # Create final prompt
            prompt = f"""Extract coffee product details from this text. Pay special attention to the PRODUCT TITLE for determining if this is a blend or single origin coffee, and consider any details found in the IMAGE ANALYSIS if present.
            Attempt to identify the origin, processing method, varietals, altitude, farm, producer, and tasting notes. 
            You are provided shop data, a scraped html page, and an analysis of the product image in order to extract this information.
            Extract information specific to the product, ignoring any superfluous information from the image or shop data.
            Return a JSON object with these fields:
            - is_single_origin: true/false/null (if unclear)
              - true if it's from one specific farm, producer, or region
              - false if it contains the word "blend" or mentions multiple origins
              - null if unclear
            - origin: {{"country": string/null, "region": string/null}}
            - roast_level: string/null
            - processing_method: string/null, format Proper case, primary; secondary; other where primary is "Washed", "Natural", "Honey", and secondary/other contain information about fermentation or experimental process elements.
            - varietals: list of strings, may also be called cultivars
            - altitude: string/null, format XXXX-YYYY if a range, or XXXX if a single value. Convert to masl if necessary, then remove units.
            - farm: string/null
            - producer: string/null, producer refers to the bean producer, not the local roaster.
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
            - Darker/Espresso roasts: Subtract 2-3 days
            - Lighter/Filter roasts: Add 2-3 days
            - African varietals (SL28, SL34, Ruiru 11, Batian): Add 1-2 days
            - Dense beans (high altitude >1600m): Add 1-2 days
            Return this as resting_period_days in the JSON.

            Text:
            {'\n'.join(text)}"""
            
            # Dump prompt to file
            self._dump_prompt(prompt, parent_title or "Unknown")

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
            response.setdefault('roast_level', None)
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
