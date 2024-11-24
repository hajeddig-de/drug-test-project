import json
import os

def load_config(config_path="config.json"):
    """Loads the configuration from the specified JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file '{config_path}' not found.")
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    return config
