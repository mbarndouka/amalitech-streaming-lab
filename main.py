import time
import random
import os
import multiprocessing

from config.config import config as get_config
from generator.generator import generate_batch
from generator.io import write_to_csv, ensure_directory_exists
from src.utils import logger as get_logger, handle_error
# Import the stream runner from streaming.py
from streaming.streaming import run_stream as run_spark_stream


def run_data_generator(config: dict, logger) -> None:
    """
    Continuously generates mock e-commerce transactions and writes them to CSV files.

    This function runs in a loop, simulating real-time activity by creating random
    batches of events according to the provided configuration ranges. The generated
    CSV files serve as the raw input for the streaming pipeline.

    Args:
        config (dict): The configuration dictionary containing settings for storage, streaming, and e-commerce parameters.
        logger: The application logger used to record generator events.
    """
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
        raise
    except Exception as e:
        handle_error(logger, e)


def main() -> None:
    """
    The main entry point for the e-commerce streaming simulator.

    This function spins up a separate background multiprocessing process for the
    Spark structured streaming pipeline and then executes the continuous data generator
    in the main thread. It handles keyboard interrupts to shut down both processes gracefully.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.toml")
    config = get_config(config_path)

    logger = get_logger("Orchestrator", config["logging"]["level"])

    spark_process = None

    try:
        # Start Spark Stream as a background Process
        logger.info("Starting Spark consumer process...")
        spark_process = multiprocessing.Process(target=run_spark_stream)
        spark_process.start()

        # Run the Data Generator in the main process
        run_data_generator(config, logger)

    except KeyboardInterrupt:
        logger.info("Graceful shutdown triggered by user. Waiting for Spark to exit...")
        if spark_process is not None and spark_process.is_alive():
            spark_process.join(timeout=10)
            if spark_process.is_alive():
                logger.warning("Spark process didn't exit in time, forcing termination.")
                spark_process.terminate()
                spark_process.join()
            logger.info("Spark consumer process terminated.")
    except Exception as e:
        handle_error(logger, e)


if __name__ == '__main__':
    main()