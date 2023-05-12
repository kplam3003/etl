import re
import json
import os
import datetime as dt
from multiprocessing.dummy import Pool
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd

import config
import utils
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from helpers import log_crawl_stats

logger = utils.load_logger()


STRATEGY_INDEED = "indeed"
STRATEGY_LUMINATI = "luminati"
STRATEGY_WEBWRAPPER = "webwrapper"
STRATEGY_DATASHAKE = "datashake"

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
def _crawl_reviews(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling indeed reviews on url: {url} - step_detail_id: {step_detail.step_detail_id}"
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

    def _extract_review(review):
        return {
            "reviewId": review.get("reviewUid"),
            "url": review.get("reviewUrl"),
            "cons": (review.get("cons") or {}).get("text") or "",
            "pros": (review.get("pros") or {}).get("text") or "",
            "title": (review.get("title") or {}).get("text") or "",
            "review": (review.get("text") or {}).get("text") or "",
            "rating": review.get("overallRating"),
            "location": review.get("location"),
            "country": review.get("country"),
            "date": review.get("submissionDate"),
        }

    total_reviews = []
    page_exceptions = []
    error_pages = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
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

        try:
            # Crawl
            params = {"fcountry": "ALL"}
            if page > 1:
                params["start"] = page * config.INDEED_REVIEW_PAGE_SIZE

            res = utils.request_get(
                url=url, params=params, logger=logger, unblocker=True, valid_status_codes=[200], verify=False
            )
            soup = BeautifulSoup(res.content, features="html.parser")
            initial_data = json.loads(soup.select_one("#comp-initialData").encode_contents())
            reviews = initial_data.get("reviewsList").get("items")
            reviews = list(map(_extract_review, reviews))
            total_reviews = [*total_reviews, *reviews]
            logger.info(f"INDEED REVIEW: Crawled {len(total_reviews)} reviews")

        except Exception as e:
            logger.exception(f"INDEED REVIEW: Unable to crawl jobs url: {url}")
            page_exceptions.append(e)
            error_pages.append(page)
            continue

    # update counts into DB for progress
    step_detail.item_count = len(total_reviews)
    session.commit()

    # Write to CSV to upload file
    df = pd.DataFrame(total_reviews)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded overview file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(total_reviews)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _crawl_jobs(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling indeed jobs on url: {url} - step_detail_id: {step_detail.step_detail_id}"
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

    def test_jk(jt):
        jk, url, _ = jt
        logger.info(f"Test job category: jk={jk} - url={url}")
        res = utils.request_get(url=url, logger=logger, unblocker=True, verify=False)
        soup = BeautifulSoup(res.content, features="lxml")
        has_item = soup.select_one(f"#jl_{jk}") is not None
        return jt, has_item

    def _extract_job(job):
        url_parse = urlparse(url)
        query_url = f"{url_parse.scheme or 'https'}://{url_parse.hostname}"

        job_title = job.get("title")
        jk = job.get("jobKey")

        logger.info(f"Extract job detail with url={query_url}/jobs - query={job_title}")
        try:
            res = utils.request_get(
                url=urljoin(query_url, "/jobs"),
                params={"q": job_title},
                logger=logger,
                unblocker=True,
                verify=False
            )
            soup = BeautifulSoup(res.text, features="lxml")

            jts = soup.select("#filter-job-type > ul > li > a")
            jts = list(map(lambda a: (a.attrs.get("href"), a.text), jts))
            jts = list(map(lambda jt: (jt[0], jt[1].split("\xa0")[0]), jts))
            jts = list(map(lambda jt: (jk, urljoin(url, jt[0]), jt[1]), jts))

            results = []
            if len(jts):
                with Pool(len(jts)) as p:
                    results = p.map(test_jk, jts)

            results = list(filter(lambda jt: jt[1], results or []))
            job_types = list(map(lambda jt: jt[0][2], results))

            txs = soup.select("#filter-taxo1 > ul > li > a")
            txs = list(map(lambda a: (a.attrs.get("href"), a.text), txs))
            txs = list(map(lambda tx: (tx[0], tx[1].split("\xa0")[0]), txs))
            txs = list(map(lambda tx: (jk, urljoin(url, tx[0]), tx[1]), txs))

            results = []
            if len(txs):
                with Pool(len(txs)) as p:
                    results = p.map(test_jk, txs)

            results = list(filter(lambda tx: tx[1], results or []))
            job_categories = list(map(lambda tx: tx[0][2], results))

            return {
                "title": job_title,
                "location": job.get("location"),
                "time": job.get("formattedRelativeLastUpdateTime")
                or job.get("formattedRelativeTime"),
                "url": job.get("url"),
                "job_id": jk,
                "job_types": json.dumps(job_types),
                "job_categories": json.dumps(job_categories),
            }
        except:
            logger.exception(
                f"Unable to extract job detail on url: {query_url} - params: {job_title}"
            )
            return {
                "title": job_title,
                "location": job.get("location"),
                "time": job.get("formattedRelativeLastUpdateTime")
                or job.get("formattedRelativeTime"),
                "url": job.get("url"),
                "job_id": jk,
                "job_types": json.dumps([]),
                "job_categories": json.dumps([]),
            }

    total_jobs = []
    page_exceptions = []
    error_pages = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
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
        try:
            params = {}
            if page > 1:
                # indeed using 150 as step size, even though they response only 100 per page
                params["start"] = page * (config.INDEED_JOB_PAGE_SIZE + 50)

            res = utils.request_get(
                url=url, params=params, logger=logger, unblocker=True, valid_status_codes=[200], verify=False
            )
            soup = BeautifulSoup(res.content, features="html.parser")
            initial_data = json.loads(soup.select_one("#comp-initialData").encode_contents())
            jobs = initial_data.get("jobList").get("jobs")
            country = initial_data.get("country")
            jobs = list(map(_extract_job, jobs))
            jobs = list(map(lambda x: {**x, "country": country}, jobs))
            total_jobs = [*total_jobs, *jobs]
            logger.info(f"INDEED JOB: Crawled {len(total_jobs)} jobs")

        except Exception as e:
            logger.exception(f"INDEED JOB: Unable to crawl jobs url: {url}")
            page_exceptions.append(e)
            error_pages.append(page)

    # update counts into DB for progress
    step_detail.item_count = len(total_jobs)
    session.commit()

    # Write to CSV to upload file
    df = pd.DataFrame(total_jobs)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
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
def _crawl_overview(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling indeed overview on url: {url} - step_detail_id: {step_detail.step_detail_id}"
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

    page_exceptions = []
    error_pages = []
    try:
        res = utils.request_get(url=url, logger=logger, unblocker=True, verify=False)
        soup = BeautifulSoup(res.content, features="lxml")

        def _match(reg):
            if reg:
                return reg.group(1)
            return ""

        info = {
            "name": _match(re.search(r'"companyName":"([^"]+)"', str(soup))),
            "founded": _match(re.search(r'"founded":([0-9]+)', str(soup))),
            "industry": _match(re.search(r'"industry":"([^"]+)"', str(soup))),
            "size": _match(re.search(r'"employeeRange":"([^"]+)"', str(soup))),
            "revenue": _match(re.search(r'"revenue":"([^"]+)"', str(soup))),
            "description": _match(re.search(r'"description":"([^"]+)"', str(soup))),
        }

        # update counts into DB for progress
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame([info])
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(output_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = output_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        logger.info(f"Uploaded overview file to gcs: {dst_file}")

    except Exception as e:
        logger.exception(f"INDEED OVERVIEW: Unable to crawl overview url: {url}")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, 1, error_pages


@auto_session
def _crawl_review_stats(
    url, step_detail, batch=None, step=None, session=None, **kwargs
):
    logger.info(
        f"Crawling indeed overview on url: {url} - step_detail_id: {step_detail.step_detail_id}"
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

    page_exceptions = []
    error_pages = []
    total_pages = 1
    try:
        res = utils.request_get(url=url, logger=logger, unblocker=True, verify=False)
        soup = BeautifulSoup(res.content, "html.parser")
        extracted_json = soup.select_one("#comp-initialData").encode_contents()
        review_stats_data = {}
        review_data = json.loads(extracted_json)
        review_stats_data["total_reviews"] = (
            review_data.get("reviewsFilters")
            .get("reviewsCount")
            .get("totalReviewCount")
        )
        review_stats_data["average_rating"] = (
            review_data.get("companyPageHeader").get("companyHeader").get("rating")
        )
        review_stats_data.update(
            {
                value["topicId"]: value["rating"]
                for value in review_data.get("reviewsFilters", {}).get("topics", [])
            }
        )

        # update counts into DB for progress
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame([review_stats_data])
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(output_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = output_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        logger.info(f"Uploaded review_stats file to gcs: {dst_file}")

        # Log crawl statistics to BigQuery
        log_crawl_stats(
            batch=batch,
            step_detail=step_detail,
            item_count=int(review_stats_data["total_reviews"])
        )

    except Exception as e:
        logger.exception(
            f"INDEED REVIEW_STATS: Unable to crawl review_stats url: {url}"
        )
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def _indeed_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
    meta_data = step_detail.meta_data
    if not meta_data or meta_data.get("data_type") == "review":
        # handle crawl reviews
        return _crawl_reviews(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "review_stats":
        # handle crawl review_stats
        return _crawl_review_stats(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "job":
        # handle crawl jobs
        return _crawl_jobs(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    else:
        # handle crawl overview
        return _crawl_overview(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )


def _get_crawler(strategy=STRATEGY_INDEED):
    if strategy == STRATEGY_INDEED:
        return _indeed_scraper
    else:
        raise Exception(f"Unsupported strategy: {strategy}")


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

        except:
            logger.exception(
                f"Unable to crawl step_detail: {step_detail_id} - url: {url}"
            )
            data = {
                "type": "crawl",
                "event": "fail",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
                "error": f"Something went wrong",
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
            # utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_AFTER_TASK, data, logger=logger)


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
