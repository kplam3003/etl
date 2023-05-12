import time
import logging
import requests
import datetime as dt

from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.cloud import pubsub_v1, logging_v2
from retry import retry as retry2
from typing import List, Any

import config
from database import ETLStep, ETLCompanyBatch, auto_session


def load_logger():
    logger = logging.getLogger(config.LOGGER)
    logger.setLevel(logging.INFO)

    handler_types = list(map(lambda h: type(h), logger.handlers))
    if CloudLoggingHandler not in handler_types:
        client = logging_v2.Client()
        handler = CloudLoggingHandler(client, name=config.LOGGER_NAME)
        logger.addHandler(handler)

    if logging.StreamHandler not in handler_types:
        logger.addHandler(logging.StreamHandler())

    return logger


def publish_pubsub(topic_id, payload, **kwargs):
    logger = kwargs.get("logger")
    if not logger:
        logger = load_logger()

    publisher = pubsub_v1.PublisherClient()
    project_id = config.GCP_PROJECT
    topic_path = publisher.topic_path(project_id, topic_id)

    future = publisher.publish(topic_path, payload)
    result = future.result()
    logger.info(f"Published message to {topic_path} - payload: {payload}")
    return result


def retry(times):
    def decor(func):
        def wrapper(*args, **kwargs):
            logger = kwargs.get("logger")
            if not logger:
                logger = load_logger()

            exp = 1.5
            for sleep in map(lambda x: (x + 1) * exp, range(times)):
                try:
                    logger.info(f"Executing {func.__name__}")
                    return func(*args, **kwargs)
                except:
                    logger.exception(
                        f"Cannot execute {func.__name__}, retry in {sleep}"
                    )
                    time.sleep(sleep)
            raise Exception(f"Failed to retry execute {func.__name__} {times} times")

        return wrapper

    return decor


@auto_session
def load_etl_step(batch: ETLCompanyBatch, session=None):
    # If not yet etl step for crawl is generated, then create one
    etl_step = (
        session.query(ETLStep)
        .filter(ETLStep.batch_id == batch.batch_id)
        .filter(ETLStep.step_type == "crawl")
        .first()
    )
    if not etl_step:
        etl_step = ETLStep()
        etl_step.step_name = (
            f"crawl-{batch.company_name.lower()}-{batch.source_code.lower()}"
        )
        etl_step.status = "running"
        etl_step.created_at = dt.datetime.today()
        etl_step.request_id = batch.request_id
        etl_step.batch_id = batch.batch_id
        etl_step.step_type = "crawl"
        etl_step.updated_at = dt.datetime.today()
        session.add(etl_step)
        session.commit()
    else:
        etl_step.status = "running"
        session.commit()

    return etl_step


@retry2(tries=10, delay=1, backoff=2)
def request_get(*args, logger=None, unblocker=False, valid_status_codes=None, **kwargs):
    """
    Wrapper to get http using proxy, with some logging if get fails
    """   
    if unblocker and config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY:
        kwargs["proxies"] = {
            "http": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
            "https": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
        }
    else:
        kwargs["proxies"] = {
            "http": config.LUMINATI_HTTP_PROXY,
            "https": config.LUMINATI_HTTP_PROXY,
        }

    kwargs["verify"] = False
    response = requests.get(*args, **kwargs)
    logger.info(
        f"Retrieving content on url: {response.url} - status: {response.status_code}"
    )

    if not valid_status_codes:
        return response

    if response.status_code not in valid_status_codes:
        logger.warning(f"Can't GET from this url {response.url}")
        logger.warning(f"Retrying....")
        logger.exception(
            f"Error {response.status_code}, response:{response.text[:100]}"
        )
        raise Exception(f"Error {response.status_code}, response:{response.text[:100]}")

    return response


def array_split(input: List[Any], sub_size: int) -> List[List[Any]]:
    if sub_size > len(input):
        return [input]
    sub_arrays = []
    sub_array_count = len(input) // sub_size
    if len(input) % sub_size != 0:
        sub_array_count += 1
    for i in range(sub_array_count):
        sub_array = input[i * sub_size : i * sub_size + sub_size]
        sub_arrays.append(sub_array)

    return sub_arrays
