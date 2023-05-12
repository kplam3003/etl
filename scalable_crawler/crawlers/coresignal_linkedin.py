import config
import pandas as pd
import json
import datetime as dt
import utils
import requests

from typing import Any, List, Dict
from urllib.parse import urlparse
from google.cloud import pubsub_v1
from google.cloud import storage
from pymongo import MongoClient

from utils import publish_step_detail_error, array_split
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from helpers import log_crawl_stats


logger = utils.load_logger()
DELIMITER = "\t"
STRATEGY_STATS = "coresignal_stats"
STRATEGY_EMPLOYEES = "coresignal_employees"


def _stats_crawler(
    url: str, step_detail: ETLStepDetail, batch: ETLCompanyBatch, step: ETLStep, session
):
    page_exceptions = []
    total_pages = 1
    error_pages = []

    try:
        # Get company stats from Coresignal
        company_shorthand_name = urlparse(url=str(batch.url)).path.split("/")[2]
        coresignal_url = f"https://api.coresignal.com/cdapi/v1/linkedin/company/collect/{company_shorthand_name}"
        stats_res = requests.get(
            url=coresignal_url,
            headers={
                "accept": "application/json",
                "Authorization": f"Bearer {config.CORESIGNAL_JWT}",
            },
            verify=False
        )
        if not stats_res.ok:
            log_msg = f"Can not get coresignal data for {batch.url}. {stats_res.reason}"
            logger.exception(log_msg)
            raise Exception(log_msg)
        company_stats = stats_res.json()

        # Update step_detail
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame({"data": [json.dumps(company_stats)]})
        src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(src_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = src_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(src_file, dst_file)
        logger.info(f"Uploaded overview file to gcs: {dst_file}")

        # Log crawl statistics to BigQuery
        log_crawl_stats(
            batch=batch,
            step_detail=step_detail,
            item_count=int(company_stats["employees_count"])
        )
    except Exception as e:
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


def _employees_crawler(
    url: str, step_detail: ETLStepDetail, batch: ETLCompanyBatch, step: ETLStep, session
):
    page_exceptions = []
    total_pages = 0
    error_pages = []

    # Filter to only get new employee: not existing in MongoDB or has updated from Coresignal
    employee_ids: List[int] = step_detail.meta_data.get("coresignal_employee_ids")
    crawled_employees = _check_crawled_employee(coresignal_ids=employee_ids)
    updated_employee_ids = _check_updated_employee(employees=crawled_employees)
    new_employee_ids = [item for item in employee_ids if item not in crawled_employees]
    new_employee_ids.extend(updated_employee_ids)
    
    # get the existing employees from MongoDB
    existing_employee_ids: List[Dict] = [
        item for item in employee_ids if item not in new_employee_ids
    ]
    employees = _fetch_existing_employees(existing_employee_ids)
    logger.info(
        f"Number of existing employee ids: {len(existing_employee_ids)}; "
        f"Number of actual documents fetched: {len(employees)}"
    )

    # Get employee data from Coresignal
    for employee_id in new_employee_ids:
        try:
            coresignal_url = f"https://api.coresignal.com/cdapi/v1/linkedin/member/collect/{employee_id}"
            stats_res = requests.get(
                url=coresignal_url,
                headers={
                    "accept": "application/json",
                    "Authorization": f"Bearer {config.CORESIGNAL_JWT}",
                },
                verify=False
            )
            if not stats_res.ok:
                log_msg = (
                    f"Can not get coresignal data for {batch.url}. {stats_res.reason}"
                )
                logger.exception(log_msg)
                raise Exception(log_msg)
            employees.append(json.dumps(stats_res.json()))
            total_pages += 1
        except Exception as e:
            logger.exception(f"Error happens while crawling employee: {employee_id}")
            page_exceptions.append(e)
            error_pages.append(employee_id)

    # update counts into DB for progress
    step_detail.item_count = len(employees)
    session.commit()

    # Write to CSV to upload file
    df = pd.DataFrame({"data": employees})
    src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(src_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = src_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(src_file, dst_file)
    logger.info(f"Uploaded overview file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(employees)
    )

    return page_exceptions, total_pages, error_pages


def _get_crawler(strategy: str):
    if strategy == STRATEGY_STATS:
        return _stats_crawler
    elif strategy == STRATEGY_EMPLOYEES:
        return _employees_crawler
    else:
        raise Exception(f"Unsupported strategy: {strategy}")


def _check_crawled_employee(coresignal_ids: List[int]) -> Dict[int, str]:
    """
    Get `last_updated` from MongoDB
    """
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    coll = db[config.MONGODB_CORESIGNAL_EMPLOYEES_COLLECTION]

    project = {"_id": 0, "coresignal_id": 1, "coresignal_last_updated": 1}
    filter = {"coresignal_id": {"$in": coresignal_ids}}
    results = coll.find(filter=filter, projection=project)
    crawled_employees = {
        item["coresignal_id"]: item["coresignal_last_updated"] for item in results
    }

    client.close()
    return crawled_employees


def _fetch_existing_employees(coresignal_ids: List[int]) -> Dict[str, Any]:
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    coll = db[config.MONGODB_CORESIGNAL_EMPLOYEES_COLLECTION]
    projection = {"_id": 0}
    existing_employees = []
    chunks = array_split(coresignal_ids, sub_size=1000)
    for chunk in chunks:
        filters = {"coresignal_id": {"$in": chunk}}
        cursor = coll.find(filter=filters, projection=projection)
        for doc in cursor:
            doc["existed"] = True
            existing_employees.append(json.dumps(doc))

    return existing_employees


def _member_es_dsl(employees: Dict[int, str]) -> List[int]:
    # Build ES search query for all employees
    # NOTE: Queries in this endpoint is limited to 10,000 characters
    should_queries = []
    for employee_id, last_updated in employees.items():
        query = {
            "bool": {
                "must": [
                    {"match": {"id": employee_id}},
                    {"range": {"last_updated": {"gt": last_updated}}},
                ]
            }
        }
        should_queries.append(query)
    es_query = {"query": {"bool": {"should": should_queries}}}

    # Send request to search endpoint
    res = requests.post(
        url="https://api.coresignal.com/cdapi/v1/linkedin/member/search/es_dsl",
        headers={
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config.CORESIGNAL_JWT}",
        },
        json=es_query,
        verify=False
    )
    if not res.ok:
        raise Exception(
            f"Coresignal problem: cdapi/v1/linkedin/member/search/es_dsl. {res.reason}"
        )

    updated_employee_ids: List[int] = res.json()
    return updated_employee_ids


def _check_updated_employee(employees: Dict[int, str]) -> List[int]:
    """
    Using member/search/es_dsl to check if an employee has new data.
    """
    if not employees:
        return []

    updated_employee_ids: List[int] = []
    batchs = array_split(input=list(employees.keys()), sub_size=80)
    for keys in batchs:
        batch_employees = {key: employees[key] for key in keys}
        results = _member_es_dsl(employees=batch_employees)
        updated_employee_ids.extend(results)

    return updated_employee_ids


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    strategy = task.get("strategy")
    crawler = _get_crawler(strategy)

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        try:
            page_exceptions, total_pages, error_pages = crawler(
                url, step_detail, batch=batch, step=step, session=session
            )

            # Publish Pub/Sub
            logger.info(
                f"Publishing crawled step detail {step_detail.step_detail_id} to Pub/Sub..."
            )
            data = {
                "type": "crawl",
                "event": "finish",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
            }
            data = json.dumps(data)
            data = data.encode("utf-8")
            # create a new publisher everytime a message is to be published
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_AFTER_TASK
            )
            logger.info(f"Publish message to topic: {topic_path} - payload: {data}")
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")

            if page_exceptions:
                publish_step_detail_error(
                    batch=batch,
                    step=step,
                    step_detail=step_detail,
                    error_time=dt.datetime.now(),
                    page_exceptions=page_exceptions,
                    total_pages=total_pages,
                    error_pages=error_pages,
                    session=session,
                    logger=logger,
                )

        except Exception as e:
            logger.exception(
                f"Unable to crawl step_detail: {step_detail_id} - url: {url}"
            )
            data = {
                "type": "crawl",
                "event": "fail",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
                "error": f"{type(e).__name__}: {str(e)}",
            }
            data = json.dumps(data)
            data = data.encode("utf-8")
            # create a new publisher everytime a message is to be published
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_AFTER_TASK
            )
            logger.info(f"Publish message to topic: {topic_path} - payload: {data}")
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
