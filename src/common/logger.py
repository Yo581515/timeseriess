import logging
import os


def get_logger(name: str, log_file: str = "app.log") -> logging.Logger:
    """
    Returns a logger that writes only to a file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        # Ensure logs directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # File handler only
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
        logger.addHandler(fh)

    return logger
