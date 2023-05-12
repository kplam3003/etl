import logging
import config
import time
import re
from datetime import datetime
from google.cloud import logging_v2
from google.cloud import pubsub_v1
from google.cloud.logging_v2.handlers import CloudLoggingHandler
import requests
from retry import retry as retry2
from database import ETLCompanyBatch, ETLStep, ETLStepDetail
from core.error_util import publish_error_message
from typing import List, Any


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
    project_id = config.GCP_PROJECT_ID
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


@retry2(tries=5, delay=1, backoff=2)
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
    logger.info(f"arguments: {args} and {kwargs}")
    response = requests.get(*args, **kwargs)
    logger.info(
        f"Retrieving content on url: {response.url} - status: {response.status_code}"
    )

    if "apple" in response.url:
        logger.info(f"Apple store crawler get review stats valid status code: {valid_status_codes}")
        logger.info(f"Apple store crawler get review stats response text: {response.text}")

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


def publish_step_detail_error(
    batch: ETLCompanyBatch,
    step: ETLStep,
    step_detail: ETLStepDetail,
    error_time,
    page_exceptions,
    total_pages,
    error_pages,
    session=None,
    logger=None,
):
    """
    Construct a valid payload and publish to error topic
    """
    # parse for exceptions
    if len(error_pages) / total_pages >= 0.5:
        severity = "MAJOR"
    else:
        severity = "MINOR"
    # get necessary info
    data_type = batch.meta_data["data_type"]
    data_version = batch.data_version
    info_dict = {"total_pages": total_pages, "error_pages": error_pages}
    total_step_details_count = (
        session.query(ETLStepDetail)
        .filter(ETLStepDetail.step_id == step.step_id)
        .count()
    )
    # convert error_time to timestamp
    if isinstance(error_time, datetime):
        error_timestamp = int(error_time.timestamp())
    else:
        error_timestamp = error_time

    publish_error_message(
        project_id=config.GCP_PROJECT_ID,
        topic_id=config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR,
        company_datasource_id=batch.company_datasource_id,
        batch_id=batch.batch_id,
        step_detail_id=step_detail.step_detail_id,
        company_id=batch.company_id,
        company_name=batch.company_name,
        source_id=batch.source_id,
        source_name=batch.source_name,
        data_type=data_type,
        error_source=step.step_type,
        error_code="IN_PROGRESS",
        severity=severity,
        exception=page_exceptions[-1],
        total_step_details=total_step_details_count,
        error_time=error_timestamp,
        data_version=data_version,
        additional_info=info_dict,
        logger=logger,
    )


def camel_to_snake_case(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


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
