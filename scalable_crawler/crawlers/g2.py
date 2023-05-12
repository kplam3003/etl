import os
import config
import pandas as pd
import json
import datetime as dt

from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1, storage

import utils
from database import (
    ETLStepDetail,
    ETLCompanyBatch,
    ETLStep,
    auto_session,
)
from utils import publish_step_detail_error
from helpers import log_crawl_stats
import requests
from retry import retry as retry2

logger = utils.load_logger()

DELIMITER = "\t"
G2_PAGE_SIZE = 25

STRATEGY_G2 = "g2"
STRATEGY_LUMINATI = "luminati"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"


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

    if not valid_status_codes:
        return response

    if response.status_code not in valid_status_codes:
        logger.warning(f"Can't GET from this url {response.url}")
        logger.warning(f"Retrying....")
        logger.exception(
            f"Error {response.status_code}, response:{response.text[:100]}"
        )

    return response

def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


@auto_session
def _post_crawl_handler(
    total_comments, src_file=None, step_detail=None, step=None, batch=None, session=None
):

    step_detail.item_count = len(total_comments)
    session.commit()
    *path, filename = src_file.split("/")

    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(src_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

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
        "item_count": len(total_comments),
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


def publish_progess(batch, step, step_detail, index, pages):
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
    utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_PROGRESS, data, logger=logger)


@auto_session
def _crawl_review_stats(url, step_detail, batch=None, step=None, session=None):
    total_pages = 1
    page_exceptions = []
    error_pages = []
    try:
        review_stats = {"total_reviews": 0, "average_rating": 0}
        response = request_get(
            url=url, logger=logger, unblocker=True, valid_status_codes=[200], verify=False
        )
        soup = BeautifulSoup(response.text, "html.parser")
        elm_rc = soup.select_one(
            "span[itemprop='aggregateRating'] meta[itemprop='reviewCount']"
        )
        if elm_rc:
            review_count = elm_rc.attrs["content"]
            review_stats["total_reviews"] = review_count

        elm_rv = soup.select_one(
            "span[itemprop='aggregateRating'] meta[itemprop='ratingValue']"
        )
        if elm_rv:
            rating_value = elm_rv.attrs["content"]
            review_stats["average_rating"] = rating_value

        # update counts into DB for progress
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame([review_stats], index=[0])
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
            item_count=review_stats["total_reviews"],
        )

    except Exception as e:
        logger.exception(f"Can't crawl review_stats from url {url} error {e}")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def _crawl_reviews(url, step_detail, batch=None, step=None, session=None):
    def _extract_reviews(soup: BeautifulSoup):
        # NOTE: Do we need split review into different aspect: like, dislike, benefit
        page_reviews = []
        elm_reviews = soup.select("#reviews div[itemprop='review']")
        for elm_review in elm_reviews:
            elm_data: dict = json.loads(
                elm_review.attrs["data-track-in-viewport-options"]
            )

            response_id = int(elm_data["survey_response_id"])

            title = None
            elm_title = elm_review.select_one("h3")
            if elm_title:
                title = elm_title.get_text(strip=True)

            review_text = None
            for item in elm_review.select(".formatted-text .spht"):
                item.decompose()
            elm_texts = [
                item.get_text(strip=True)
                for item in elm_review.select(".formatted-text")
            ]
            review_text = ". ".join(elm_texts)

            rating_value = None
            elm_star = elm_review.select_one(".stars")
            if elm_star and elm_star.has_attr("class"):
                vals = [c for c in elm_star["class"] if c.startswith("stars-")]
                if vals:
                    rating_value = int(vals[0].split("-")[1])

            published_date = (
                datetime.strptime(elm_data["published_date"], "%Y%m%d")
                .date()
                .isoformat()
            )

            data = {
                "id": response_id,
                "title": title,
                "review_text": review_text,
                "rating_value": rating_value,
                "date": published_date,
            }
            page_reviews.append(data)
        return page_reviews

    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
    total_reviews = []
    page_exceptions = []
    error_pages = []
    for index, page in enumerate(pages):
        # Publish progress
        publish_progess(batch, step, step_detail, index, pages)
        try:
            logger.info(
                f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%"
            )
            logger.info(f"Crawling url: {url} with page {page}")

            page_url = urlparse(url=url)._replace(fragment="").geturl()
            page_url = f"{page_url}?page={page}"
            response = request_get(
                url=page_url, logger=logger, unblocker=True, valid_status_codes=[200], verify=False
            )
            soup = BeautifulSoup(response.text, "html.parser")
            page_reviews = _extract_reviews(soup)
            total_reviews.extend(page_reviews)

        except Exception as e:
            logger.exception(f"Can't crawl this page {page} with url {url} error {e}")
            page_exceptions.append(e)
            error_pages.append(page)

    df = pd.DataFrame(total_reviews)
    df["review_text"] = df["review_text"].str.replace("\n", ". ")
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)
    _post_crawl_handler(
        total_reviews,
        src_file=output_file,
        step_detail=step_detail,
        step=step,
        batch=batch,
        session=session,
    )

    # Log crawl statistics to BigQuery
    log_crawl_stats(batch=batch, step_detail=step_detail, item_count=len(total_reviews))

    return page_exceptions, total_pages, error_pages


@auto_session
def _luminati_crawl(url, step_detail, batch=None, step=None, session=None):
    meta_data = step_detail.meta_data
    data_type: str = meta_data["data_type"]
    if data_type == "review":
        return _crawl_reviews(url, step_detail, batch=batch, step=step, session=session)
    elif data_type == "review_stats":
        return _crawl_review_stats(
            url, step_detail, batch=batch, step=step, session=session
        )
    else:
        raise ValueError(f"Un-supported data_type=`{data_type}`")


def _get_crawler(strategy=STRATEGY_LUMINATI):
    return _luminati_crawl


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    strategy = task.get("strategy")

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        # Crawl data from source website
        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        crawl = _get_crawler(strategy=strategy)
        try:
            page_exceptions, total_pages, error_pages = crawl(
                url, step_detail, batch=batch, step=step
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
                "error": f"{type(e).__name__}: {str(e)}"[:200],
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

    logger.info(f"Crawl task {task} success")


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
