import logging
import sys
from logger_global import logger

def validate_payload_fields(payload, mandatory_fields):
    """
    Validate payload for company_datasource flow.
    """
    missings = []
    for field in mandatory_fields:
        if field not in payload:
            missings.append(field)

    if missings:
        logger.warning(f"Missing payload fields: ({', '.join(missings)})")
        return False

    return True


def init_local_logger(log_name):
    """
    simple local logger for testing
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - {log_name} - %(filename)s : %(lineno)d - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    return logger


class CompanyExistsAndRunningError(Exception):
    """
    Custom class for when company_datasource_id has already exists
    and has status not in (finished, completed with error)
    """
    pass
