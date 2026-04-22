import os
import csv
from generator.io import write_to_csv
from generator.generator import generate_batch

def test_write_to_csv(tmp_path):
    """Test that data is correctly formatted and written to a CSV file."""
    test_dir = str(tmp_path / "test_dir")
    os.makedirs(test_dir)

    actions = ["view", "add_to_cart", "purchase"]
    products = ["shoes", "socks", "t-shirt"]
    batch = generate_batch(10, actions, products)

    filepath = write_to_csv(batch, test_dir)

    assert os.path.exists(filepath)
    assert filepath.endswith(".csv")

    with open(filepath, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

        assert len(rows) == 10

        expected_headers = ["event_id", "user_id", "action", "products", "event_timestamp"]
        assert reader.fieldnames == expected_headers

        assert rows[0]["event_id"] == batch[0]["event_id"]
        assert rows[0]["action"] == batch[0]["action"]