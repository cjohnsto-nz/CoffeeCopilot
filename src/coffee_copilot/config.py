import yaml

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Get roaster URLs
ROASTER_URLS = {name: data['url'] for name, data in config['roasters'].items()}
