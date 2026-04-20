import logging
from typing import NoReturn

def logger(name:str, level:str) ->  logging.Logger:
    """Returns a logger with the specified name and level."""
    log = logging.getLogger(name)

    if not log.handlers:
        log.setLevel(getattr(logging, level.upper(), logging.INFO))
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log

def handle_error(logger: logging.Logger, error: Exception) -> NoReturn:
    """Handles an error and logs it."""
    logger.critical(
        f"An error occurred:{str(error)}", exc_info=True
    )