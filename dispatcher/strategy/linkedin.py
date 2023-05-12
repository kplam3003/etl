from database import (
    ETLCompanyBatch,
    ETLStep,
    ETLStepDetail,
    ETLDatasource,
    auto_session,
)
import utils
import requests
import config
import time
from bs4 import BeautifulSoup
import math
import datetime as dt
import json
from google.cloud import pubsub_v1
from jobstats import JobStats

STRATEGY_LINKEDIN = "linkedin"
STRATEGY_LUMINATI = "luminati"

LINKEDIN_URL = "https://www.linkedin.com"

logger = utils.load_logger()


headers = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
}


@utils.retry(5)
def request_get(*args, **kwargs):
    res = requests.get(*args, **kwargs)
    if res.status_code != 200:
        raise Exception(f"Unable to fetch content, status_code: {res.status_code}")
    return res


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


@auto_session
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_LINKEDIN,
    session=None,
    data_type="job",
    start_id=1,
    lang="en",
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

    pages = math.ceil(total / config.LINKEDIN_JOB_PAGE_SIZE) or 1
    steps = math.ceil(total / config.ROWS_PER_STEP) or 1
    paging = math.ceil(config.ROWS_PER_STEP / config.LINKEDIN_JOB_PAGE_SIZE)

    etl_step = load_etl_step(batch, session=session)

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
def luminati_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    data_type = batch.meta_data.get("data_type")
    if data_type not in ["overview"]:
        raise Exception(
            f"Unsupported data_type: {data_type} on luminati data collector"
        )

    source_type = "linkedin_company_profile"

    # 1. Check whether datasource for this batch existed yet, create one if not
    etl_datasource = (
        session.query(ETLDatasource)
        .filter(ETLDatasource.batch_id == batch.batch_id)
        .first()
    )
    if not etl_datasource:
        # Call webhook api to generate one
        logger.info(f"Creating new luminati job for batch_id: {batch.batch_id}")
        res = requests.post(
            f"{config.WEBHOOK_API}/luminati",
            json={"url": batch.url, "source_type": source_type},
            headers={"Authorization": config.WEBHOOK_TOKEN},
            timeout=30
        )

        if res.status_code != 200:
            logger.info(
                f"[ERROR] Unable to create luminati job for batch_id: {batch.batch_id}"
            )
            raise Exception(
                f"Unable to create luminati job - status: {res.status_code} - response: {res.content}"
            )

        body = res.json()

        # Update to database
        etl_datasource = ETLDatasource(
            source_name=batch.source_name,
            source_url=batch.url,
            batch_id=batch.batch_id,
            provider=STRATEGY_LUMINATI,
            provider_job_id=body.get("collection_id"),
            created_at=dt.datetime.utcnow(),
            updated_at=dt.datetime.utcnow(),
        )
        session.add(etl_datasource)
        session.commit()

    # 2. check whether data have been sent from luminati
    collection_id = etl_datasource.provider_job_id
    res = requests.get(
        f"{config.WEBHOOK_API}/luminati/{collection_id}",
        headers={"Authorization": config.WEBHOOK_TOKEN},
        timeout=30,
    )

    # Data not yet arrived
    if res.status_code == 404:
        logger.info(
            f"Luminati job id {collection_id} not yet initialized, republishing..."
        )

        # 3. If provider source is not yet been initial crawled, then publish message to self topic for later process.
        payload = json.dumps({"batch_id": batch.batch_id})
        payload = payload.encode("utf-8")
        topic_id = config.GCP_PUBSUB_TOPIC_DISPATCH
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.GCP_PROJECT, topic_id)
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(f"Published message to {topic_id}, result: {future.result()}")
        time.sleep(15)
        return

    if res.status_code != 200:
        logger.info(f"[ERROR] Unable to luminati job for batch_id: {batch.batch_id}")
        raise Exception(
            f"Unable to create luminati job - status: {res.status_code} - response: {res.content}"
        )

    body = res.json()
    total = body.get("total")
    logger.info(
        f"Dispatching task with reviews - batch_id: {batch.batch_id} - count: {total}"
    )
    dispatch_tasks(
        total, batch, data_type=data_type, strategy=STRATEGY_LUMINATI, session=session
    )


@auto_session
def linkedin_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    today = dt.datetime.today()
    options = {"headers": headers}

    if config.LUMINATI_HTTP_PROXY:
        options["proxies"] = {
            "http": config.LUMINATI_HTTP_PROXY,
            "https": config.LUMINATI_HTTP_PROXY,
        }

    url = batch.url
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type") if meta_data else "job"

    if data_type not in ["job"]:
        raise Exception(f"LinkedIn data_type={data_type} is not supported yet.")

    try:
        res = request_get(
            url, params={"geoId": "92000000", "location": "Worldwide"}, **options, verify=False
        )
        soup = BeautifulSoup(res.content, features="lxml")
        result = soup.select_one("div.results-context-header")
        total = result.select_one("span.results-context-header__job-count").string
        total = int(total)

        dispatch_tasks(
            total, batch, session=session, data_type="job", url=url, start_id=0
        )
    except:
        logger.exception(f"Unable to find company id from provided url: {url}")
        raise

    # Log crawl statistics to BigQuery
    n_jobs = 0
    job_stats = JobStats(proxy_url=config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY)
    try:
        n_jobs = job_stats.get_linkedin_job_count(url=batch.url)
        job_stats.log_crawl_stats(batch=batch, item_count=n_jobs)
    except:
        pass


def get_scraper(strategy=STRATEGY_LINKEDIN):
    if strategy == STRATEGY_LINKEDIN:
        return linkedin_scrape
    elif strategy == STRATEGY_LUMINATI:
        return luminati_scrape
    else:
        raise Exception(f"Not implemented strategy {strategy}")
