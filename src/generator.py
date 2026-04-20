import random
import uuid
from datetime import datetime
from typing import List, Dict, Any

def generate_events(actions: List[str], products:List[str]) -> Dict[str, Any]:
    """Generates a list of events."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(100, 999),
        "action":random.choice(actions),
        "products":random.choice(products),
        "event_timestamp": datetime.now().isoformat() + "Z"
    }

def generate_batch(num_events:int, actions:List[str], products:List[str]) -> List[Dict[str, Any]]:
    """Generates a batch of events."""
    return [
        generate_events(actions, products) for _ in range(num_events)
    ]