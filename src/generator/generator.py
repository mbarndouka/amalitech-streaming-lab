import random
import uuid
from datetime import datetime
from typing import List, Dict, Any

def generate_events(actions: List[str], products:List[str]) -> Dict[str, Any]:
    """
    Generates a single synthetic e-commerce event.

    Args:
        actions (List[str]): List of possible user actions (e.g., 'view', 'purchase').
        products (List[str]): List of available product identifiers.

    Returns:
        Dict[str, Any]: A dictionary representing a single event containing
                        an event_id, user_id, action, product, and timestamp.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(100, 999),
        "action":random.choice(actions),
        "products":random.choice(products),
        "event_timestamp": datetime.now().isoformat() + "Z"
    }

def generate_batch(num_events:int, actions:List[str], products:List[str]) -> List[Dict[str, Any]]:
    """
    Generates a batch of synthetic e-commerce events.

    Args:
        num_events (int): The number of events to generate.
        actions (List[str]): List of possible user actions.
        products (List[str]): List of available product identifiers.

    Returns:
        List[Dict[str, Any]]: A list containing the generated event dictionaries.
    """
    return [
        generate_events(actions, products) for _ in range(num_events)
    ]