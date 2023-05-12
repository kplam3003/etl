import json
import math
import datetime as dt
import json
import re

import requests
from retry import retry

import config
import utils
from database import (
    ETLCompanyBatch,
    ETLStep,
    ETLStepDetail,
    auto_session,
)


STRATEGY_GARTNER = "gartner"
GARTNER_URL = "https://www.gartner.com/reviews/api2-proxy/reviews/market/vendor/filter"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"
logger = utils.load_logger()


@retry(tries=10, delay=1, backoff=2)
def request_get(*args, **kwargs):
    """
    Make request to BrightData WEBUNBLOCKER PROXY to get HTML
    """
    if config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY:
        kwargs["proxies"] = {
            "http": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
            "https": config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
        }
    kwargs["verify"] = False
    res = requests.get(*args, **kwargs)
    logger.info(f"Retrieving content on url: {res.url} - status: {res.status_code}")
    if res.status_code != 200:
        logger.warning(f"Can't get Gartner html from this url {res.url}")
        logger.warning(f"Retrying....")
        logger.exception(f"Error {res.status_code}, response:{res.text[:100]}")
        raise Exception(f"Error {res.status_code}, response:{res.text[:100]}")
    return res


@auto_session
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_GARTNER,
    session=None,
    data_type="review",
    start_id=1,
    lang="eng",
    url=None,
):
    today = dt.datetime.today()
    url = url or batch.url
    logger.info(f"Total items found for url {url}: {total}")
    meta_data = batch.meta_data or {}
    paging_info = meta_data.get("paging_info", {})
    by_langs = paging_info.get("by_langs", {})
    by_langs[lang] = total
    paging_info["by_langs"] = by_langs
    paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
    meta_data["paging_info"] = paging_info
    batch.meta_data = meta_data
    session.commit()
    page_size = config.GARTNER_REVIEW_PAGE_SIZE
    pages = math.ceil(total / page_size) or 1
    steps = math.ceil(total / config.ROWS_PER_STEP) or 1
    paging = math.ceil(config.ROWS_PER_STEP / page_size)
    etl_step = utils.load_etl_step(batch, session=session)
    logger.info(f"Generating step details, there are {steps} steps")

    for step in range(steps):
        start = step * paging + 1
        end = (list(range(pages + 1)[start : start + paging]) or [1])[-1]
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = f"{data_type}-{etl_step.step_name}-{today.strftime('%Y%m%d')}-{lang}-f{step + start_id + 1:03d}".strip()
        step_detail.status = "waiting"
        step_detail.step_id = etl_step.step_id
        step_detail.batch_id = batch.batch_id
        step_detail.file_id = step + start_id + 1
        step_detail.paging = f"{start}:{end}"
        step_detail.created_at = today
        step_detail.updated_at = today
        step_detail.progress_total = end + 1 - start
        step_detail.progress_current = 0
        step_detail.lang = lang
        step_detail.meta_data = {
            "version": "1.0",
            "data_type": data_type,
            "url": url,
            "lang": lang,
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
        utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_CRAWL, payload, logger=logger)
    logger.info(f"Assign task completed")


@auto_session
def gartner_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    today = dt.datetime.today()
    url = batch.url
    meta_data = batch.meta_data
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
        step_detail.paging = f"1:1"
        step_detail.created_at = today
        step_detail.updated_at = today
        step_detail.progress_total = 1
        step_detail.progress_current = 0
        step_detail.lang = "en"
        step_detail.meta_data = {
            "version": "1.0",
            "data_type": "review_stats",
            "url": url,
            "lang": "en",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": url,
            "strategy": STRATEGY_GARTNER,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "review":
        logger.info(f"Extracting reviews info on url: {url}")
        market = re.search(r"/market/([^/]+)", url).group(1)
        vendor = re.search(r"/vendor/([^/]+)", url).group(1)
        product = re.search(r"/product/([^/]+)", url).group(1)
        start_index = 1
        end_index = 15
        params = (
            ("vendorSeoName", vendor),
            ("marketSeoName", market),
            ("productSeoName", product),
            ("startIndex", start_index),
            ("endIndex", end_index)
        )
        response = request_get(GARTNER_URL, params=params, verify=False)
        body = json.loads(response.text)
        total = body.get("totalCount")
        logger.info(f"Found {total} reviews on url: {url}")
        dispatch_tasks(
            total,
            batch,
            strategy=STRATEGY_GARTNER,
            session=session,
            data_type="review",
            url=url,
        )
    else:
        raise Exception(f"Not supported Gartner data type: {data_type}")


def get_scraper(strategy=STRATEGY_GARTNER):
    if strategy == STRATEGY_GARTNER:
        return gartner_scrape
    else:
        raise Exception(f"Not supported strategy: {strategy}")
