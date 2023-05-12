import math
import datetime as dt
import json
from multiprocessing.dummy import Pool
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

import config
import utils
from database import (
    ETLCompanyBatch,
    ETLStepDetail,
    auto_session,
)
from jobstats import JobStats

STRATEGY_INDEED = "indeed"
STRATEGY_LUMINATI = "luminati"
STRATEGY_WEBWRAPPER = "webwrapper"
STRATEGY_DATASHAKE = "datashake"

INDEED_URL = "https://{}.indeed.com"

logger = utils.load_logger()


headers = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
}


@auto_session
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_INDEED,
    session=None,
    data_type="review",
    start_id=0,
    lang="eng",
    url=None,
):
    today = dt.datetime.today()
    url = url or batch.url

    logger.info(f"Total reviews found for url {url}: {total}")
    meta_data = batch.meta_data or {}
    paging_info = meta_data.get("paging_info", {})
    by_langs = paging_info.get("by_langs", {})
    by_langs[lang] = total
    paging_info["by_langs"] = by_langs
    paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
    meta_data["paging_info"] = paging_info
    batch.meta_data = meta_data
    session.commit()

    page_size = (
        config.INDEED_REVIEW_PAGE_SIZE
        if data_type == "review"
        else config.INDEED_JOB_PAGE_SIZE
    )
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


@utils.retry(5)
def request_get(*args, **kwargs):
    res = requests.get(*args, **kwargs)
    if res.status_code != 200:
        raise Exception(f"Unable to fetch content, status_code: {res.status_code}")
    return res


@auto_session
def indeed_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    today = dt.datetime.today()
    options = {"headers": headers}

    if config.LUMINATI_HTTP_PROXY:
        options["proxies"] = {
            "http": config.LUMINATI_HTTP_PROXY,
            "https": config.LUMINATI_HTTP_PROXY,
        }

    # handle
    url = batch.url
    path = urlparse(url).path
    global_url = urljoin(INDEED_URL.format("www"), path)
    logger.info(f"Dispatching url: {url}")
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type")
    logger.info(
        f"Dispatching Indeed crawl task, data_type: {data_type}, url {global_url}"
    )

    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)

        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"review_stats-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
            "url": global_url,
            "lang": "en",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": global_url,
            "strategy": STRATEGY_INDEED,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "overview":
        # handle for overview
        path = urlparse(url).path
        global_url = urljoin(INDEED_URL.format("www"), path)

        etl_step = utils.load_etl_step(batch, session=session)

        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"overview-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
            "data_type": "overview",
            "url": global_url,
            "lang": "en",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": global_url,
            "strategy": STRATEGY_INDEED,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "job":
        # handle for job
        path = urlparse(url).path
        total_job_count = 0

        def _dispatch_job_crawl(domain):
            local_url = urljoin(INDEED_URL.format(domain), path)
            logger.info(f"Crawling page info on {local_url}")
            try:
                res = utils.request_get(url=local_url, logger=logger, unblocker=True, valid_status_codes=[200, 404], verify=False)
                if res.status_code == 404:
                    return None
                soup = BeautifulSoup(res.text, "html.parser")
                init_data = json.loads(soup.select_one("#comp-initialData").encode_contents())
                job_count = int(init_data["jobList"]["totalJobCount"])
                return job_count, domain, local_url
            except:
                logger.exception(f"Unable to extract job count from url: {url}")

        with Pool(10) as p:
            results = p.map(_dispatch_job_crawl, config.INDEED_COUNTRIES)
            if not len(list(filter(lambda item: item is not None, results))):
                raise Exception(
                    f"Unable to process url: {url} - data_type: {data_type} - batch_id: {batch.batch_id}"
                )

            # total_job_count = 0
            for index, item in enumerate(results, 1):
                if not item:
                    continue

                job_count, domain, local_url = item
                total_job_count += int(job_count)

                if job_count <= 0:
                    continue

                dispatch_tasks(
                    job_count,
                    batch,
                    strategy=STRATEGY_INDEED,
                    session=session,
                    data_type="job",
                    lang=domain,
                    url=local_url,
                    start_id=index * 1000,
                )

            if total_job_count == 0:
                raise Exception(
                    f"Unable to process url: {url} - data_type: {data_type} - batch_id: {batch.batch_id} - Error: No jobs found."
                )

        # Log crawl statistics to BigQuery
        job_stats = JobStats(proxy_url=config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY)
        try:
            # Leverage `total_job_count`, no need to call `job_stats.get_indeed_job_count`
            job_stats.log_crawl_stats(batch=batch, item_count=total_job_count)
        except:
            pass

    elif data_type == "review":
        # default to crawl review
        path = urlparse(url).path
        global_url = urljoin(INDEED_URL.format("www"), path)
        try:
            res = utils.request_get(url=url, logger=logger, unblocker=True, valid_status_codes=[200], verify=False)
            soup = BeautifulSoup(res.text, "html.parser")
            init_data = json.loads(soup.select_one("#comp-initialData").encode_contents())
            review_count = init_data["reviewsFilters"]["reviewsCount"]["totalReviewCount"]
            dispatch_tasks(
                review_count,
                batch,
                strategy=STRATEGY_INDEED,
                session=session,
                data_type="review",
                lang="en",
                url=global_url,
            )
        except:
            logger.exception(
                f"Unable to extract review count from url: {url} - data_type: {data_type} - batch_id: {batch.batch_id}"
            )
            raise

    else:
        raise Exception(f"Unsupported Indeed data_type: {data_type}")


def get_scraper(strategy=STRATEGY_INDEED):
    if strategy == STRATEGY_INDEED:
        return indeed_scrape
    else:
        raise Exception(f"Unsupported strategy {strategy}")
