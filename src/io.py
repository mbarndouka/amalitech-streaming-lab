import csv
import os
from datetime import datetime
from typing import List, Dict, Any

def ensure_directory_exists(filepath: str) -> None:
    """Ensures that the directory for the given filepath exists."""
    os.makedirs(filepath, exist_ok=True)
    return filepath


def write_to_csv(events: List[Dict[str, Any]], output_dir:str) -> str:
    """Writes the given data to a CSV file."""
    if not events:
        return ""

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filepath = os.path.join(output_dir, f"events_{current_time}.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=events[0].keys())
        writer.writeheader()
        writer.writerows(events)
    return filepath


