import sys
import os
import logging

import tqdm
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler

import config

# CONSTs
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev_dq_logger')
ENABLE_CLOUD_LOGGING = int(os.environ.get('ENABLE_CLOUD_LOGGING', '0')) != 0


# own logger
class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)  


def create_logger(logger_name=config.LOGGER_NAME,
                  console=True,
                  to_file=False,
                  progress_bar=False,
                  console_level=logging.INFO,
                  file_level=logging.DEBUG,
                  progress_bar_level=logging.INFO):
    # create a base logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # common formatter
    log_formatter = logging.Formatter(
        fmt='%(asctime)s\t%(filename)s\t%(funcName)s\t%(levelname)s\t%(message)s',
    )

    if console:
        # create console handler with a higher log level
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(console_level)
        ch.setFormatter(log_formatter)
        # add console handler to log
        logger.addHandler(ch)

    if to_file:
        os.makedirs(config.LOGGER_DIR, exist_ok=True)
        log_file = f"{config.LOGGER_DIR}/{logger_name}.log"
        # create file handler which logs even debug messages
        fh = logging.FileHandler(log_file)
        fh.setLevel(file_level)
        fh.setFormatter(log_formatter)
        # add the handlers to logger
        logger.addHandler(fh)

    if progress_bar:
        tqdm_handler = TqdmLoggingHandler()
        tqdm_handler.setLevel(progress_bar_level)
        tqdm_handler.setFormatter(log_formatter)
        logger.addHandler(tqdm_handler)
    
    return logger


# leo-etl logger
def init_logger(logger_name, name, level=logging.INFO):
    # Instantiates a client
    logger = logging.getLogger(name)
    logger.setLevel(level)
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


