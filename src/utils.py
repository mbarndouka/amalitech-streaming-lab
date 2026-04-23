import logging
from typing import NoReturn

def logger(name:str, level:str) ->  logging.Logger:
    """
    Creates and configures a logger instance with a standard format.

    If the logger does not already have handlers, a StreamHandler is added
    with a specific formatting style.

    Args:
        name (str): The name to assign to the logger.
        level (str): The logging level, e.g., 'INFO' or 'DEBUG'.

    Returns:
        logging.Logger: The configured logging instance.
    """
    log = logging.getLogger(name)

    if not log.handlers:
        log.setLevel(getattr(logging, level.upper(), logging.INFO))
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log

def handle_error(logger: logging.Logger, error: Exception) -> NoReturn:
    """
    Logs a critical exception with traceback details.

    Args:
        logger (logging.Logger): The logger to record the error.
        error (Exception): The exception that was raised.
    """
    logger.critical(
        f"An error occurred:{str(error)}", exc_info=True
    )