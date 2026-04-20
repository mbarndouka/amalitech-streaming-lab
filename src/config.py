import tomllib
from typing import Dict, Any

def config(filepath: str) -> Dict[str, Any]:
    """Reads the config file."""
    with open(filepath, "rb") as f:
        return tomllib.load(f)
