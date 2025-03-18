# Coffee Copilot

An AI-powered coffee recommendation system that helps you discover new specialty coffees based on your order history and preferences.

## Project Structure

```
coffee_copilot/
├── src/
│   └── coffee_copilot/         # Main package
│       ├── __init__.py
│       ├── ai_coffee_extractor.py  # AI-powered coffee detail extraction
│       ├── app.py                  # Scraping orchestration
│       ├── config.py              # Configuration management
│       ├── database.py            # Database models and operations
│       ├── enhance_products.py    # Product enhancement logic
│       ├── order_manager.py       # Order history management
│       └── recommend_coffee.py    # Recommendation engine
├── data/                       # SQLite database and other data files (created at runtime)
│   ├── coffee_data.db
├── logs/                      # Log files and extraction prompts (created at runtime)
│   ├── prompts
│   │   ├── extractions
│   │   └── recommendations
│   └── recommendations
├── utils/                     # Utility scripts
│   ├── add_order_temp.py     # Add orders manually
│   └── show_options.py       # Display available coffees
├── .env                      # Environment variables (create from template)
├── config.yaml              # Configuration values
├── requirements.txt         # Python dependencies
├── setup.py                # Package setup and installation
└── run_pipeline.py         # Main entry point
```

## Features

- Scrapes coffee products from specialty roasters
- Uses AI to extract and enhance coffee details
- Tracks order history and spending patterns
- Provides personalized coffee recommendations
- Ensures variety in recommendations
- Prevents duplicate recommendations of previously ordered coffees

## Setup

1. Create and activate a Python virtual environment:
```bash
# Create a virtual environment in the project directory
python -m venv venv

# Activate the virtual environment
# On Windows:
.\venv\Scripts\activate
# On Unix/MacOS:
source venv/bin/activate

# Verify activation - your prompt should show (venv)
# You should see the virtual environment path when running:
python -c "import sys; print(sys.prefix)"
```

2. Install the package and its dependencies:
```bash
# Install required GitHub packages first
pip install git+https://github.com/practical-data-science/ShopifyScraper.git

# Install from requirements.txt for development
pip install -r requirements.txt

# OR install the package directly (includes all dependencies)
pip install -e .
```

3. Configure your settings in `config.yaml`

4. Create a `.env` file with your Azure OpenAI credentials:
```
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_API_VERSION=your_api_version
AZURE_OPENAI_DEPLOYMENT=your_deployment_name
```

5. Run the pipeline:
```bash
python run_pipeline.py
```

## Utility Scripts

- `utils/add_order_temp.py`: Manually add orders to your history
- `utils/show_options.py`: See available coffees you haven't tried

## Development

The project uses a standard Python package structure with `setup.py`. Core functionality is in the `coffee_copilot` package under `src/`. 

To install in development mode:
```bash
pip install -e .
```

This will install all required dependencies:
- pandas
- sqlalchemy
- openai
- python-dotenv
- pyyaml
- beautifulsoup4
- requests
