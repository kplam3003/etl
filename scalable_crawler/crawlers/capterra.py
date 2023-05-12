import requests
import config
import logging
import re
import json
import datetime as dt
import pandas as pd

from typing import Callable
from retry import retry
from google.cloud import storage, pubsub_v1

import utils
from database import ETLStep, ETLStepDetail, ETLCompanyBatch, auto_session
from helpers import log_crawl_stats

logger = utils.load_logger()

DELIMITER = "\t"

HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36",
    "authority": "www.capterra.com",
    # 'lum-auth-token': config.LUMI_AUTH_TOKEN,
}
REVIEW_API = "https://www.capterra.com/spotlight/rest/reviews"

params = {}
if config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY:
    params["proxies"] = {
        "http": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
        "https": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
    }
    params["verify"] = False


@retry(tries=6, delay=1, backoff=3)
def _request_get(check_fn: Callable[[str], bool] = None, *args, **kwargs):
    if config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY:
        kwargs["proxies"] = {
            "http": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
            "https": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
        }
    kwargs["verify"] = False
    res = requests.get(*args, **kwargs)
    logger.info(f"Info(url={res.url}, status={res.status_code})")

    if res.status_code != 200:
        msg = f"Error(url={res.url}, response={res.text[:100]})"
        logger.exception(msg)
        raise ValueError(msg)
    elif (check_fn != None) and (not check_fn(res.text)):
        msg = f"Error(url={res.url}, reason='Unexpected content!')"
        logger.warning(msg)
        raise ValueError(msg)

    return res


@utils.retry(5)
def request_get(*args, **kwargs):
    kwargs["verify"] = False
    res = requests.get(*args, **kwargs)
    if res.status_code != 200:
        raise Exception(f"Unable to fetch content, status_code: {res.status_code}")
    return res


def crawl_review_stats(url, step_detail, batch=None, step=None, session=None):
    pid = pid = int(str(url).split("/")[4])
    url = f"https://www.capterra.com/spotlight/rest/product?productId={pid}"
    keys_to_get = [
        "reviewsTotal",
        "overallRating",
        "easeOfUseRating",
        "customerServiceRating",
        "functionalityRating",
        "valueForMoneyRating",
        "recommendationRating",
    ]

    def _check_product_response(content: str) -> bool:
        try:
            _ = json.loads(content)
            return True
        except json.decoder.JSONDecodeError:
            return False

        return False

    logger.info(
        f"Crawling capterra review_stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    total_pages = 1
    page_exceptions = []
    error_pages = []

    try:
        response = _request_get(url=url, headers=HEADERS, check_fn=_check_product_response, verify=False)
        data = response.json()
        output_dict = {utils.camel_to_snake_case(k): data.get(k) for k in keys_to_get}

    except Exception as e:
        logger.exception(f"Unable to crawl url: {url}")
        page_exceptions.append(e)
        error_pages.append(0)

    # write progress to db
    # save crawled item count
    step_detail.item_count = 1
    session.commit()

    final_df = pd.DataFrame([output_dict])

    # write CSV file locally
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    final_df.to_csv(output_file, index=False, sep=DELIMITER)

    # upload csv to GCS
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=int(output_dict[utils.camel_to_snake_case("reviewsTotal")])
    )

    return page_exceptions, total_pages, error_pages


def crawl(url, step_detail, batch=None, step=None, session=None):
    pid = int(str(url).split("/")[4])
    total_comments = []

    def _check_reviews_response(content: str) -> bool:
        try:
            _ = json.loads(content)
            return True
        except json.decoder.JSONDecodeError:
            return False

        return False

    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
    page_exceptions = []
    error_pages = []
    for index, page in enumerate(pages):
        # Publish progress
        logger.info(
            f"Publishing crawling step detail {step_detail.step_detail_id} progress..."
        )
        data = {
            "type": "crawl",
            "event": "progress",
            "batch": batch.to_json(),
            "step": step.to_json(),
            "step_detail": step_detail.to_json(),
            "progress": {"total": len(pages), "current": index + 1},
        }
        data = json.dumps(data)
        data = data.encode("utf-8")
        # create a new publisher everytime a message is to be published
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_PROGRESS
        )
        future = publisher.publish(topic_path, data)
        future.result()
        logger.info(f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%")

        # Crawl
        from_index = (page - 1) * config.CAPTERRA_PAGE_SIZE
        url = f"{REVIEW_API}?apiVersion=2&productId={pid}&from={from_index}&size={config.CAPTERRA_PAGE_SIZE}"
        try:
            response = _request_get(url=url, headers=HEADERS, check_fn=_check_reviews_response, verify=False)

            data = response.json()
            mylist = data.get("hits")
            logger.info(f"Crawling from {url}. Length of list results: {len(mylist)}")
            total_comments = [*total_comments, *mylist]

        except Exception as e:
            logger.exception(f"Unable to crawl url: {url}")
            page_exceptions.append(e)
            error_pages.append(page)
            continue

    # write progress to db
    # save crawled item count
    step_detail.item_count = len(total_comments)
    session.commit()

    final_df = pd.DataFrame(total_comments)
    if "reviewer" in final_df.columns:
        final_df["reviewer"] = final_df["reviewer"].apply(json.dumps)

    if "vendorResponse" in final_df.columns:
        final_df["vendorResponse"] = final_df["vendorResponse"].apply(json.dumps)

    # write CSV file locally
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    final_df.to_csv(output_file, index=False, sep=DELIMITER)

    # upload csv to GCS
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(total_comments)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def capterra_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
    meta_data = step_detail.meta_data
    if not meta_data or meta_data.get("data_type") == "review":
        # handle crawl reviews
        return crawl(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "review_stats":
        return crawl_review_stats(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    else:
        raise Exception(f"Not supported type={meta_data}")


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    crawler = capterra_scraper

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        # Crawl data from source website
        try:
            page_exceptions, total_pages, error_pages = capterra_scraper(
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
                utils.publish_step_detail_error(
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
            logging.exception(
                f"Unable to crawl step_detail: {step_detail.step_detail_id}", e
            )
            step_detail.status = "completed with error"
            step_detail.updated_at = dt.datetime.now()
            session.commit()
            raise e


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
