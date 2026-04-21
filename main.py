import time
import random
import os
import multiprocessing

from src.config import config as get_config
from src.generator import generate_batch
from src.io import write_to_csv, ensure_directory_exists
from src.utils import logger as get_logger, handle_error
# Import the stream runner from streaming.py
from src.streaming import run_stream as run_spark_stream


def run_data_generator(config: dict, logger) -> None:
    """Runs the data generation loop."""
    storage_cfg = config["storage"]
    stream_cfg = config["stream"]
    ecommerce_cfg = config["ecommerce"]

    ensure_directory_exists(storage_cfg["output_dir"])
    logger.info(f"Starting data generator writing to: {storage_cfg['output_dir']}")

    try:
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
    except KeyboardInterrupt:
        logger.info("Data generator shut down by user.")
    except Exception as e:
        handle_error(logger, e)


def main() -> None:
    """Application entry point."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.toml")
    config = get_config(config_path)

    logger = get_logger("Orchestrator", config["logging"]["level"])

    try:
        # Start Spark Stream as a background Process
        logger.info("Starting Spark consumer process...")
        spark_process = multiprocessing.Process(target=run_spark_stream)
        spark_process.start()

        # Run the Data Generator in the main process
        run_data_generator(config, logger)

    except KeyboardInterrupt:
        logger.info("Graceful shutdown triggered by user.")
        if 'spark_process' in locals() and spark_process.is_alive():
            spark_process.terminate()
            spark_process.join()
            logger.info("Spark consumer process terminated.")
    except Exception as e:
        handle_error(logger, e)


if __name__ == '__main__':
    main()