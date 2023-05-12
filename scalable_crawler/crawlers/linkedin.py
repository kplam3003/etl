import config
import pandas as pd
from google.cloud import storage
import re
import json
import os
import datetime as dt
import utils
import requests
from multiprocessing.dummy import Pool
from urllib.parse import unquote
from bs4 import BeautifulSoup
from database import (
    ETLStepDetail,
    ETLCompanyBatch,
    ETLStep,
    ETLDatasource,
    auto_session,
)

from google.cloud import pubsub_v1
from helpers import log_crawl_stats

logger = utils.load_logger()


STRATEGY_LINKEDIN = "linkedin"
STRATEGY_LUMINATI = "luminati"

LINKEDIN_URL = "https://www.linkedin.com"
LINKEDIN_SEARCH_URL = (
    "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
)

DELIMITER = "\t"


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


@utils.retry(5)
def request_get(*args, **kwargs):
    kwargs["verify"] = False
    res = requests.get(*args, **kwargs)
    if res.status_code != 200:
        raise Exception(f"Unable to fetch content, status_code: {res.status_code}")
    return res


@auto_session
def _crawl_jobs(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling linkedin jobs on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    options = {
        "headers": {
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        }
    }

    if config.LUMINATI_HTTP_PROXY:
        options["proxies"] = {
            "http": config.LUMINATI_HTTP_PROXY,
            "https": config.LUMINATI_HTTP_PROXY,
        }

    def _extract_job(element):
        try:
            job_id = element.attrs.get("data-id")
            if not job_id:
                match = re.search(r"urn:li:jobPosting:([0-9]+)", str(element))
                if match:
                    job_id = match.group(1)
            title_link = element.select_one(".result-card__full-card-link")
            if not title_link:
                title_link = element.select_one(".base-card__full-link")

            job_url = title_link.attrs.get("href")
            _res = request_get(job_url, verify=False, **options)
            _soup = BeautifulSoup(_res.text, features="lxml")
            data = _soup.select_one('script[type="application/ld+json"]')
            data = json.loads(data.contents[-1])

            job_location = data.get("jobLocation").get("address")
            job_function = ""
            try:
                job_function = _soup.select('li[class="job-criteria__item"] > span')[
                    2
                ].text
            except:
                pass

            return {
                "job_id": job_id,
                "job_url": job_url,
                "title": data.get("title"),
                "posted_date": data.get("datePosted"),
                "job_type": data.get("employmentType"),
                "job_function": job_function,
                "location": job_location.get("addressLocality"),
                "country": job_location.get("addressCountry"),
            }
        except:
            logger.exception(f"Unable to process: {element}")

    total_jobs = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))

    cids = re.search(r"f_C=([^&]+)", url).group(1)
    cids = unquote(cids)
    cids = cids.split(",")

    total_pages = len(cids) * len(pages)
    page_exceptions = []
    error_pages = []

    for company_id in cids:
        logger.info(f"Crawling linkedin jobs for company_id: {company_id}")

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
            utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_PROGRESS, data, logger=logger)
            logger.info(
                f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%"
            )

            # Crawl
            try:
                res = request_get(
                    LINKEDIN_SEARCH_URL,
                    params={
                        "f_C": company_id,
                        "geoId": "92000000",
                        "start": (page - 1) * config.LINKEDIN_JOB_PAGE_SIZE,
                    },
                    verify=False,
                    **options
                )
                soup = BeautifulSoup(res.text, features="lxml")
                elements = soup.select("li")

                if len(elements) == 0:
                    logger.info(
                        f"No more jobs found for linkedin company_id: {company_id}"
                    )
                    break

                items = []
                with Pool(len(elements)) as p:
                    items = p.map(_extract_job, elements)
                items = list(filter(lambda x: x is not None, items))
                total_jobs = [*total_jobs, *items]
                logger.info(f"Crawled {len(total_jobs)} job items")

            except Exception as e:
                logger.exception(
                    f"LINKEDIN JOB: Unable to crawl jobs with company_id {company_id}, "
                    f"page {page}, search url {LINKEDIN_SEARCH_URL}"
                )
                page_exceptions.append(e)
                error_pages.append(page)

    # save crawled item count
    step_detail.item_count = len(total_jobs)
    session.commit()

    # write CSV file locally
    df = pd.DataFrame(total_jobs)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)

    # upload csv to GCS
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(total_jobs)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _luminati_crawl(url, step_detail, batch=None, step=None, session=None):
    etl_datasource = (
        session.query(ETLDatasource)
        .filter(ETLDatasource.batch_id == batch.batch_id)
        .first()
    )
    if not etl_datasource:
        raise Exception(
            f"Unable to find datasource info for step_detail {step_detail.step_detail_id}"
        )

    collection_id = etl_datasource.provider_job_id

    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_reviews = []
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
        utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_PROGRESS, data, logger=logger)
        logger.info(f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%")

        # Crawl
        res = requests.get(
            f"{config.WEBHOOK_API}/luminati/{collection_id}",
            headers={"Authorization": config.WEBHOOK_TOKEN},
            params={"page": page},
            verify=False
        )
        body = res.json()
        reviews = body.get("data")
        total_reviews = [*total_reviews, *reviews]

    df = pd.DataFrame(total_reviews)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)
    return output_file, total_reviews


@auto_session
def _linkedin_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
    meta_data = step_detail.meta_data or {}
    if meta_data.get("data_type") == "job":
        # handle crawl jobs
        return _crawl_jobs(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    else:
        # handle crawl overview
        raise Exception(f'Unsupported data_type={meta_data.get("data_type")}')


def _get_crawler(strategy=STRATEGY_LINKEDIN):
    logger.info(f"Use crawler strategy {strategy}")
    if strategy == STRATEGY_LINKEDIN:
        return _linkedin_scraper
    elif strategy == STRATEGY_LUMINATI:
        return _luminati_crawl
    else:
        raise Exception(f"Unsupported strategy: {strategy}")


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    strategy = task.get("strategy", STRATEGY_LINKEDIN)
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
            logger.exception(
                f"[ERROR] Unable to crawl step detail {step_detail.step_detail_id}"
            )
            data = {
                "type": "crawl",
                "event": "fail",
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


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
