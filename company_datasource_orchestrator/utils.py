import logging
import config
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler


def load_logger():
    logger = logging.getLogger(config.LOGGER)
    logger.setLevel(logging.INFO)

    handler_types = list(map(lambda h: type(h), logger.handlers))
    if CloudLoggingHandler not in handler_types:
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client, name=config.LOGGER_NAME)
        logger.addHandler(handler)

    if logging.StreamHandler not in handler_types:
        logger.addHandler(logging.StreamHandler())
  
    return logger
