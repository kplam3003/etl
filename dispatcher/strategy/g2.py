import math
import json
import datetime as dt

from urllib import parse
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1

import config
import utils
# from utils import request_get
from database import (
    ETLCompanyBatch,
    ETLStepDetail,
    auto_session,
)
import requests
from retry import retry as retry2


STRATEGY_G2 = "g2"
STRATEGY_LUMINATI = "luminati"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"
logger = utils.load_logger()


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


@auto_session
def dispatch_tasks(
    total_pages, batch: ETLCompanyBatch, strategy=STRATEGY_LUMINATI, session=None
):
    today = dt.datetime.today()
    url = batch.url
    logger.info(f"Total pages found for url {url}: {total_pages}")
    meta_data = batch.meta_data or {}
    data_type = meta_data.get("data_type")

    if data_type == "review_stats":
        logger.info(f"Extracting review stats info on url: {url}")
        etl_step = utils.load_etl_step(batch, session=session)

        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"review_stats-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}"
        )
        step_detail.status = "waiting"
        step_detail.step_id = etl_step.step_id
        step_detail.batch_id = batch.batch_id
        step_detail.file_id = 1
        step_detail.paging = "1:1"
        step_detail.created_at = today
        step_detail.updated_at = today
        step_detail.progress_total = 1
        step_detail.progress_current = 0
        step_detail.lang = "en"
        step_detail.meta_data = {
            "data_type": data_type,
            "url": url,
            "lang": "en",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": url,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "review":
        meta_data["paging_info"] = {
            "total": total_pages,
            "by_langs": {"en": total_pages},
        }
        batch.meta_data = meta_data
        session.commit()
        page_size = config.G2_PAGE_SIZE
        total = total_pages * page_size
        pages = total_pages
        steps = math.ceil(total / config.ROWS_PER_STEP) or 1
        paging = math.ceil(config.ROWS_PER_STEP / page_size)
        etl_step = utils.load_etl_step(batch, session=session)
        logger.info(f"Generating step details, there are {steps} steps")
        publisher = pubsub_v1.PublisherClient()
        project_id = config.GCP_PROJECT
        topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
        topic_path = publisher.topic_path(project_id, topic_id)
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start : start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip()
            step_detail.status = "waiting"
            step_detail.step_id = etl_step.step_id
            step_detail.batch_id = batch.batch_id
            step_detail.file_id = step + 1
            step_detail.paging = f"{start}:{end}"
            step_detail.created_at = today
            step_detail.updated_at = today
            step_detail.progress_total = end + 1 - start
            step_detail.progress_current = 0
            step_detail.lang = "en"
            step_detail.meta_data = {
                "data_type": data_type,
                "url": url,
                "lang": "en",
            }
            session.add(step_detail)
            session.commit()
            payload = json.dumps(
                {
                    "url": url,
                    "batch_id": batch.batch_id,
                    "strategy": strategy,
                    "ids": [step_detail.step_detail_id],
                }
            )
            payload = payload.encode("utf-8")
            logger.info(f"Publishing a message to {topic_id}: {payload}")
            future = publisher.publish(topic_path, payload)
            logger.info(f"Published message to {topic_id}, result: {future.result()}")

    else:
        raise Exception(f"Not supported Gartner data type: {data_type}")

    logger.info(f"Assign task completed")


@auto_session
def g2_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    raise NotImplemented("G2 raw source strategy for G2 is not supported yet")


@auto_session
def luminati_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    def get_total_page_g2(soup: BeautifulSoup) -> int:
        elm_links = soup.select("a[class*='pagination']")

        if len(elm_links) == 0:
            return 1

        last_page_link: str = elm_links[-1].attrs["href"]
        query_params = parse.parse_qs(parse.urlsplit(last_page_link).query)
        last_page = int(query_params["page"][0])
        return last_page

    response = request_get(
        url=batch.url,
        logger=logger,
        unblocker=True,
        valid_status_codes=[200],
        verify=False
    )
    soup = BeautifulSoup(response.text, "html.parser")
    total_pages = get_total_page_g2(soup)
    dispatch_tasks(total_pages, batch, strategy=STRATEGY_LUMINATI, session=session)


def get_scraper(strategy=STRATEGY_LUMINATI):
    if strategy == STRATEGY_LUMINATI:
        return luminati_scrape
    else:
        return g2_scrape
