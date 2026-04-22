import os
from config.config import config as get_config
from src.utils import logger as get_logger
from config.spark_session import get_spark_session
from streaming.pipeline import build_pipeline
from src.utils import handle_error


def run_stream() -> None:
    """Runs the stream."""
    # Ensure config path is absolute relative to project root so the separate process finds it
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(base_dir, "config.toml")

    config = get_config(config_path)
    logger = get_logger("Orchestrator", config["logging"]["level"])

    try:
        logger.info("Starting Spark session")
        spark = get_spark_session(
            # Fixed Key Path: Access app_name through the 'spark' dictionary
            app_name=config["spark"]["app_name"],
            jdbc_package=config["spark"]["jdbc_package"]
        )

        build_pipeline(spark, config)

    except KeyboardInterrupt:
        logger.info("Graceful shutdown triggered by user.")
    except Exception as e:
        handle_error(logger, e)