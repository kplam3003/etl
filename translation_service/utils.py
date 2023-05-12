import logging
import config
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler


def load_logger():
    logger = logging.getLogger(config.LOGGER)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")

    handler_types = list(map(lambda h: type(h), logger.handlers))
    if CloudLoggingHandler not in handler_types:
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client, name=config.LOGGER_NAME)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if logging.StreamHandler not in handler_types:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
    
    return logger
