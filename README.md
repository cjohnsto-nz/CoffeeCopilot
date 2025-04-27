# Coffee Copilot

An AI-powered coffee recommendation system that helps you discover new specialty coffees based on your order history and preferences.


https://github.com/user-attachments/assets/0953f802-1d17-4a72-abfb-cff60136bc9a


## Demo



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
│       ├── recommend_coffee.py    # Recommendation engine
│       └── utils/                 # Utility modules
│           ├── __init__.py
│           └── db_migration.py    # Database migration helpers
├── data/                       # SQLite database and other data files (created at runtime)
│   ├── coffee_data_new.db      # Database with URL-based product identification
├── logs/                      # Log files and extraction prompts (created at runtime)
│   ├── prompts
│   │   ├── extractions
│   │   └── recommendations
│   └── recommendations
├── migrations/                # Alembic database migrations
│   ├── versions/              # Migration script versions
│   ├── env.py                # Alembic environment configuration
│   ├── README.md             # Migration documentation
│   └── script.py.mako        # Migration script template
├── .env                      # Environment variables (create from template)
├── alembic.ini               # Alembic configuration
├── config.yaml               # Configuration values
├── migrate_order_history.py  # Script to migrate order history between databases
├── requirements.txt         # Python dependencies
├── setup.py                # Package setup and installation
└── run_pipeline.py         # Main entry point
```

## Features

- Scrapes coffee products from specialty roasters
- Uses AI to extract and enhance coffee details
- Tracks order history and spending patterns
- Provides personalized coffee recommendations with contextual awareness
- Ensures variety in recommendations
- Prevents duplicate recommendations of previously ordered coffees
- Uses URL-based product identification to prevent duplication
- Preserves variant IDs to maintain order history integrity
- Captures user feedback to improve future recommendations
- Supports optional blacklisting of rejected recommendations

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

## Database Management

### Migrations

The project uses Alembic for database migrations:

```bash
# Apply all pending migrations
alembic upgrade head

# Create a new migration
alembic revision --autogenerate -m "Description of changes"
```

### Order History Migration

To migrate order history from the old database to the new one:

```bash
python migrate_order_history.py
```

This script intelligently matches products and variants between databases to preserve all order history.

## Recommendation System

The recommendation system has been enhanced with several improvements:

### Conversation Context

The system maintains conversation context between recommendations in the same session using OpenAI's chat API. This allows the AI to remember previous recommendations and user feedback, leading to more personalized suggestions over time.

### User Feedback Loop

When a user rejects a recommendation, the system:
1. Captures feedback on why the coffee wasn't appealing
2. Stores this feedback for future reference
3. Uses the feedback to inform the next recommendation
4. Optionally allows blacklisting the coffee

### Data Integrity

The system now uses URL-based product identification to prevent duplication issues and preserve variant IDs across scraper runs. This ensures that order history relationships remain intact and recommendations are based on accurate data.

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
- alembic
