import sys
import os
import logging
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler

LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev_logger')
ENABLE_CLOUD_LOGGING = int(os.environ.get('ENABLE_CLOUD_LOGGING', '0')) != 0


def init_logger(logger_name, name):
    # Instantiates a client
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - {name} - %(filename)s : %(lineno)d - %(message)s")

    handler_types = list(map(lambda h: type(h), logger.handlers))
    if CloudLoggingHandler not in handler_types and ENABLE_CLOUD_LOGGING:
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client, name=logger_name)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if logging.StreamHandler not in handler_types:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
    
    return logger


def auto_logger(name):
    def decorator(func):
        def wrapper(*args, logger=None, **kwargs):
            if not logger:
                logger = init_logger(LOGGER_NAME, name if type(name) == str else name.__name__)
            logger.info(f"Executing {func.__name__}...")
            return func(*args, **kwargs, logger=logger)
        return wrapper
    
    if type(name) == str:
        return decorator
    else:
        return decorator(name)
