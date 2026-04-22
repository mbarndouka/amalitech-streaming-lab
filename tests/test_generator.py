from generator.generator import generate_batch, generate_events

ACTIONS = ["view", "add_to_cart", "purchase"]
PRODUCTS = ["shoes", "socks", "t-shirt"]

def test_generate_events():
    """Test that a single event has the exact correct dictionary structure."""
    events = generate_events(ACTIONS, PRODUCTS)

    expected_keys = ["event_id", "user_id", "action", "products", "event_timestamp"]

    assert set(events.keys()) == set(expected_keys)

    #Check that all expected keys exist
    assert isinstance(events["event_id"], str)
    assert isinstance(events["user_id"], int)
    assert isinstance(events["action"], str)
    assert isinstance(events["products"], str)
    assert isinstance(events["event_timestamp"], str)


    assert events["action"] in ACTIONS
    assert events["products"] in PRODUCTS

def test_generate_batch():
    """Test that a batch of events has the correct number of events."""
    batch_size = 10
    batch = generate_batch(batch_size, ACTIONS, PRODUCTS)


    assert len(batch) == batch_size
    assert isinstance(batch, list)
    assert isinstance(batch[0], dict)

