import time
import random
import os

from  src.config import config as get_config
from src.generator import generate_batch
from  src.io import write_to_csv, ensure_directory_exists
from  src.utils import logger as get_logger, handle_error

def run_stream(config:dict, logger) -> None:
    """Runs the stream."""
    storage_cfg = config["storage"]
    stream_cfg = config["stream"]
    ecommerce_cfg = config["ecommerce"]

    ensure_directory_exists(storage_cfg["output_dir"])
    logger.info(f"Starting stream with config: {storage_cfg['output_dir']}")
    while True:
        batch_size = random.randint(
            stream_cfg["min_event_per_file"],
            stream_cfg["max_event_per_file"]
        )

        events = generate_batch(
            batch_size,
            ecommerce_cfg["actions"],
            ecommerce_cfg["products"]
        )
        filepath = write_to_csv(events, storage_cfg["output_dir"])
        logger.info(f"Wrote {len(events)} events to {filepath}")
        time.sleep(stream_cfg["sleep_internal_sec"])

def main()-> None:
    """Application entry point."""
    # Ensure working directory is set to root project folder
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config = get_config(os.path.join(base_dir,"config.toml"))

    # config = get_config("config.toml")
    logger = get_logger("Orchestrator", config["logging"]["level"])

    try:
        run_stream(config, logger)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown triggered by user.")
    except Exception as e:
        handle_error(logger, e)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
