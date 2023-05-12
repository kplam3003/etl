import json
import math
import os
import re
import random
import praw
import requests
import datetime as dt
import js2py

from retry import retry
from urllib import parse
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from google_play_scraper import app
from typing import Callable
from itertools import product
from urllib.parse import urlparse, parse_qs, urlsplit

import utils
import config
from database import ETLRequest, ETLCompanyBatch, ETLStep, ETLStepDetail, auto_session
from strategy import g2, upload, glassdoor, indeed, linkedin, gartner, ambitionbox, coresignal_linkedin
from core.error_util import publish_error_message
from strategy.applestore import AppStore


logger = utils.load_logger()
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36"


@retry(tries=6, delay=1, backoff=3)
def request_get(check_fn: Callable[[str], bool] = None, *args, **kwargs):
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


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


@auto_session
def generate_steps(batch: ETLCompanyBatch, session=None):
    if batch.source_type == "csv":
        # Handle for csv type
        generate_uploaded_csv_steps(batch, session=session)
    elif batch.source_code.lower() == "googleplay":
        generate_googleplay_steps(batch, session=session)
    elif batch.source_code.lower() == "capterra":
        generate_capterra_steps(batch, session=session)
    elif batch.source_code.lower() == "applestore":
        generate_applestore_steps(batch, session=session)
    elif batch.source_code.lower() == "trustpilot":
        generate_trustpilot_steps(batch, session=session)
    elif batch.source_code.lower() == "g2":
        generate_g2_steps(batch, session=session)
    elif batch.source_code.lower() == "reddit":
        generate_reddit_steps(batch, session=session)
    elif batch.source_code.lower() == "glassdoor":
        generate_glassdoor_steps(batch, session=session)
    elif batch.source_code.lower() == "indeed":
        generate_indeed_steps(batch, session=session)
    elif batch.source_code.lower() == "linkedin":
        generate_linkedin_steps(batch, session=session)
    elif batch.source_code.lower() == "gartner":
        generate_gartner_steps(batch, session=session)
    elif batch.source_code.lower() == "mouthshut":
        generate_mouthshut_steps(batch, session=session)
    elif batch.source_code.lower() == "ambitionbox":
        generate_ambitionbox_steps(batch, session=session)
    elif batch.source_code.lower() == "coresignal linkedin":
        generate_coresignal_linkedin_steps(batch, session=session)
    else:
        logger.error(f"Not supported source: {batch.source_code}")
        raise Exception(f"Not supported source {batch.source_code}")


@auto_session
def generate_uploaded_csv_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = upload.get_scraper(strategy=upload.STRATEGY_CSV)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_googleplay_steps(batch: ETLCompanyBatch, session=None):
    def _fetch_total_reviews(app_id):
        """Takes in a app_id, returns the number of reviews"""
        total = app(app_id, country="al", lang="af").get("reviews", 0)
        logger.info(f"Checked first comb : [al, af] : {total} reviews")
        language_codes = list(
            map(lambda item: item[1], config.GOOGLEPLAY_SUPPORTED_LANGS)
        )
        choices = random.choices(
            list(product(config.GOOGLEPLAY_COUNTRIES, language_codes)), k=10
        )
        for i, comb in enumerate(choices):
            try:
                result = app(app_id, lang=comb[1], country=comb[0]).get(
                    "reviews", 0)
                total = max(total, result)
                logger.info(f"Checked comb {i}: {comb} : {result} reviews")
            except Exception as e:
                logger.exception(f"Error: {e}")
        return total

    today = dt.datetime.today()
    url = batch.url
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type")
    logger.info(
        f"Dispatching Google Play crawl task, data_type: {data_type}, url {url}"
    )

    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)

        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "review":
        etl_step = utils.load_etl_step(batch, session=session)
        # Fetch total reviews
        match = re.search(r"id=([^&]+)", url)
        app_id = match.group(1)
        total = _fetch_total_reviews(app_id)
        meta_data = batch.meta_data or {}
        paging_info = meta_data.get("paging_info", {})
        by_langs = paging_info.get("by_langs", {})
        by_langs["all"] = total
        paging_info["by_langs"] = by_langs
        paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
        meta_data["paging_info"] = paging_info
        batch.meta_data = meta_data
        session.commit()

        pages = math.ceil(total / 200) or 1
        steps = math.ceil(total / config.ROWS_PER_STEP) or 1
        paging = math.ceil(config.ROWS_PER_STEP / 200)

        logger.info(f"Generating step details, there are {steps} steps")
        publisher = pubsub_v1.PublisherClient()
        project_id = config.GCP_PROJECT
        topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
        topic_path = publisher.topic_path(project_id, topic_id)

        step_detail_list = []
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start: start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip(
            )
            step_detail.status = "waiting"
            step_detail.step_id = etl_step.step_id
            step_detail.batch_id = batch.batch_id
            step_detail.file_id = step + 1
            step_detail.paging = f"{start}:{end}"
            step_detail.created_at = today
            step_detail.updated_at = today
            step_detail.progress_current = 0
            step_detail.progress_total = (end + 1) - start
            step_detail.lang = "en"
            step_detail.meta_data = {
                "version": "1.0",
                "data_type": "review",
                "url": url,
                "lang": "vi",
            }
            session.add(step_detail)
            session.commit()
            step_detail_list.append(step_detail.step_detail_id)

        payload = json.dumps(
            {"url": url, "batch_id": batch.batch_id, "ids": step_detail_list}
        )
        payload = payload.encode("utf-8")
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(
            f"Published message to {topic_id}, result: {future.result()}")

    else:
        raise Exception(f"Unsupported Google Play data_type: {data_type}")

    logger.info("Assign task completed")


@auto_session
def generate_capterra_steps(batch: ETLCompanyBatch, session=None):
    today = dt.datetime.today()
    headers = {
        "User-Agent": USER_AGENT,
        "authority": "www.capterra.com",
    }

    def _check_product_response(content: str) -> bool:
        try:
            _ = json.loads(content)
            return True
        except json.decoder.JSONDecodeError:
            return False

        return False

    url: str = batch.url
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type")
    logger.info(
        f"Dispatching Capterra crawl task, data_type: {data_type}, url {url}")
    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
        pid = int(url.split("/")[4])
        response = request_get(
            url=f"https://www.capterra.com/spotlight/rest/product?productId={pid}",
            check_fn=_check_product_response,
            headers=headers,
        )
        body = response.json()
        total = min(body.get("reviewsTotal"), config.CAPTERRA_MAX_ITEMS)
        meta_data = batch.meta_data or {}
        paging_info = meta_data.get("paging_info", {})
        by_langs = paging_info.get("by_langs", {})
        by_langs["all"] = total
        paging_info["by_langs"] = by_langs
        paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
        meta_data["paging_info"] = paging_info
        batch.meta_data = meta_data
        session.commit()

        pages = math.ceil(total / config.CAPTERRA_PAGE_SIZE) or 1
        steps = math.ceil(total / config.ROWS_PER_STEP) or 1
        paging = math.ceil(config.ROWS_PER_STEP /
                           config.CAPTERRA_PAGE_SIZE) or 1

        etl_step = utils.load_etl_step(batch, session=session)

        logger.info(f"Generating step details, there are {steps} steps")
        publisher = pubsub_v1.PublisherClient()
        project_id = config.GCP_PROJECT
        topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
        topic_path = publisher.topic_path(project_id, topic_id)

        step_detail_list = []
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start: start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip(
            )
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
                "version": "1.0",
                "data_type": data_type,
                "url": url,
                "lang": "vi",
            }
            session.add(step_detail)
            step_detail_list.append(step_detail)
            session.commit()

            payload = json.dumps(
                {
                    "url": url,
                    "batch_id": batch.batch_id,
                    "ids": [step_detail.step_detail_id],
                }
            )
            payload = payload.encode("utf-8")
            logger.info(f"Publishing a message to {topic_id}: {payload}")
            future = publisher.publish(topic_path, payload)
            logger.info(
                f"Published message to {topic_id}, result: {future.result()}")

    else:
        raise Exception(f"Unsupported Capterra data_type: {data_type}")

    logger.info("Assign task completed")


@auto_session
def generate_mouthshut_steps(batch: ETLCompanyBatch, session=None):
    today = dt.datetime.today()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36"
    }

    def _get_total_review(soup: BeautifulSoup) -> int:
        total_review = 1
        anchor = "var query_string"
        for elm in soup.select("script"):
            content = elm.decode_contents().strip()
            if anchor in content:
                lines = [line.strip()
                         for line in content.split(";") if anchor in line]
                query_string: str = js2py.eval_js(lines[0])
                params = parse.parse_qs(query_string)
                total_review = int(params["forcount"][0])
                break
        return total_review

    # make request to get total review and pages available
    url = batch.url
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type")
    logger.info(
        f"Dispatching Mouthshut crawl task, data_type: {data_type}, url {url}")

    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
            "data_type": "review_stats",
            "url": url,
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
        response = request_get(url=url, headers=headers)

        soup = BeautifulSoup(response.text, "html.parser")
        total_review = _get_total_review(soup)

        # save review and paging info to batch meta_data
        meta_data = batch.meta_data or {}
        paging_info = meta_data.get("paging_info", {})
        by_langs = paging_info.get("by_langs", {})
        by_langs["all"] = total_review
        paging_info["by_langs"] = by_langs
        paging_info["total"] = sum(by_langs.values())
        meta_data["paging_info"] = paging_info
        batch.meta_data = meta_data
        session.commit()
        # calculate number of step details
        pages = math.ceil(
            total_review / config.MOUTHSHUT_REVIEW_PAGE_SIZE) or 1
        steps = math.ceil(total_review / config.ROWS_PER_STEP) or 1
        paging = math.ceil(config.ROWS_PER_STEP /
                           config.MOUTHSHUT_REVIEW_PAGE_SIZE)
        # create etl steps
        etl_step = utils.load_etl_step(batch, session=session)
        # generate step details
        logger.info(f"Generating step details, there are {steps} steps")
        publisher = pubsub_v1.PublisherClient()
        project_id = config.GCP_PROJECT
        topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
        topic_path = publisher.topic_path(project_id, topic_id)

        step_detail_list = []
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start: start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip(
            )
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
                "data_type": "review",
                "url": url,
            }
            session.add(step_detail)
            session.commit()

            step_detail_list.append(step_detail.step_detail_id)

        payload = json.dumps(
            {"url": url, "batch_id": batch.batch_id, "ids": step_detail_list}
        )
        payload = payload.encode("utf-8")
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(
            f"Published message to {topic_id}, result: {future.result()}")

    else:
        raise Exception(f"Unsupported Mouthshut data_type: {data_type}")

    logger.info("Assign task completed")


@auto_session
def generate_trustpilot_steps(batch: ETLCompanyBatch, session=None):
    today = dt.datetime.today()
    headers = {
        "User-Agent": USER_AGENT
    }

    url = batch.url
    parsed = urlparse(url)
    url = "https://www.trustpilot.com" + parsed.path
    data_type = batch.meta_data.get("data_type")
    logger.info(
        f"Dispatching Trustpilot crawl task, data_type: {data_type}, url {url}")

    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
        response = request_get(
            url=url,
            params={"languages": "all"},
            headers=headers,
        )

        soup = BeautifulSoup(response.text, "html.parser")
        js_content = soup.select_one("script#__NEXT_DATA__").decode_contents()
        datas = json.loads(js_content)

        total_review = datas["props"]["pageProps"]["businessUnit"]["numberOfReviews"]
        meta_data = batch.meta_data or {}
        paging_info = meta_data.get("paging_info", {})
        by_langs = paging_info.get("by_langs", {})
        by_langs["all"] = total_review
        paging_info["by_langs"] = by_langs
        paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
        meta_data["paging_info"] = paging_info
        batch.meta_data = meta_data
        session.commit()

        pages = math.ceil(total_review / config.TRUSTPILOT_PAGE_SIZE) or 1
        steps = math.ceil(total_review / config.ROWS_PER_STEP) or 1
        paging = math.ceil(config.ROWS_PER_STEP / config.TRUSTPILOT_PAGE_SIZE)

        etl_step = utils.load_etl_step(batch, session=session)

        logger.info(f"Generating step details, there are {steps} steps")
        publisher = pubsub_v1.PublisherClient()
        project_id = config.GCP_PROJECT
        topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
        topic_path = publisher.topic_path(project_id, topic_id)

        step_detail_list = []
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start: start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip(
            )
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
            }
            session.add(step_detail)
            session.commit()

            step_detail_list.append(step_detail.step_detail_id)

        payload = json.dumps(
            {"url": url, "batch_id": batch.batch_id, "ids": step_detail_list}
        )
        payload = payload.encode("utf-8")
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(
            f"Published message to {topic_id}, result: {future.result()}")

    else:
        raise Exception(f"Unsupported data_type for Trustpilot: {data_type}")

    logger.info("Assign task completed")


@auto_session
def generate_gartner_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = gartner.get_scraper(strategy=gartner.STRATEGY_GARTNER)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_g2_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = g2.get_scraper(strategy=g2.STRATEGY_LUMINATI)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_glassdoor_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = glassdoor.get_scraper(
        strategy=glassdoor.STRATEGY_GLASSDOOR)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_ambitionbox_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = ambitionbox.get_scraper(
        strategy=ambitionbox.STRATEGY_AMBITIONBOX
    )
    strategy_dispatch(batch, session=session)


@auto_session
def generate_indeed_steps(batch: ETLCompanyBatch, session=None):
    strategy_dispatch = indeed.get_scraper(strategy=indeed.STRATEGY_INDEED)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_linkedin_steps(batch: ETLCompanyBatch, session=None):

    strategy_dispatch = None

    data_type = batch.meta_data.get("data_type")
    if data_type == "overview":
        strategy_dispatch = linkedin.get_scraper(
            strategy=linkedin.STRATEGY_LUMINATI)
    else:
        strategy_dispatch = linkedin.get_scraper(
            strategy=linkedin.STRATEGY_LINKEDIN)
    strategy_dispatch(batch, session=session)


@auto_session
def generate_applestore_steps(batch: ETLCompanyBatch, session=None):
    today = dt.datetime.today()

    url: str = batch.url
    meta_data: dict = batch.meta_data
    data_type: str = meta_data.get("data_type")
    rows_per_step = 2000
    logger.info(
        f"Dispatching Apple AppStore crawl task, data_type: {data_type}, url {url}"
    )

    if data_type == "review_stats":
        etl_step = utils.load_etl_step(batch, session=session)

        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = (
            f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
            "version": "1.0",
            "data_type": "review_stats",
            "url": url,
            "lang": "vi",
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
        app_store = AppStore()
        results = app_store.get_review_count(apps_url=url)
        file_id = 0

        for path, result in results:
            total = AppStore.parse_rating_count(app_data=result)
            country_code = AppStore.parse_country_code(api_path=path)

            if total == 0:
                continue

            meta_data = batch.meta_data or {}
            paging_info = meta_data.get("paging_info", {})
            by_langs = paging_info.get("by_langs", {})
            by_langs[country_code] = total
            paging_info["by_langs"] = by_langs
            paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
            meta_data["paging_info"] = paging_info
            batch.meta_data = meta_data
            session.commit()

            pages = math.ceil(total / config.APPLESTORE_PAGE_SIZE) or 1
            steps = math.ceil(total / rows_per_step) or 1
            paging = math.ceil(rows_per_step / config.APPLESTORE_PAGE_SIZE)

            etl_step = utils.load_etl_step(batch, session=session)

            logger.info(f"Generating step details, there are {steps} steps")
            publisher = pubsub_v1.PublisherClient()
            project_id = config.GCP_PROJECT
            topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
            topic_path = publisher.topic_path(project_id, topic_id)

            step_detail_list = []
            for step in range(steps):
                file_id += 1
                start = step * paging + 1
                end = (
                    list(range(pages + 1)[start: start + paging]) or [1])[-1]
                step_detail = ETLStepDetail()
                step_detail.request_id = batch.request_id
                step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{file_id:03d}".strip(
                )
                step_detail.status = "waiting"
                step_detail.step_id = etl_step.step_id
                step_detail.batch_id = batch.batch_id
                step_detail.file_id = file_id
                step_detail.paging = f"{start}:{end}"
                step_detail.created_at = today
                step_detail.updated_at = today
                step_detail.progress_total = end + 1 - start
                step_detail.progress_current = 0
                step_detail.lang = country_code
                step_detail.meta_data = {
                    "version": "1.0",
                    "data_type": "review",
                    "url": url,
                    "country": country_code
                }
                session.add(step_detail)
                step_detail_list.append(step_detail)
                session.commit()

                payload = json.dumps(
                    {
                        "url": url,
                        "batch_id": batch.batch_id,
                        "ids": [step_detail.step_detail_id],
                    }
                )
                payload = payload.encode("utf-8")
                logger.info(f"Publishing a message to {topic_id}: {payload}")
                future = publisher.publish(topic_path, payload)
                logger.info(
                    f"Published message to {topic_id}, result: {future.result()}"
                )

    else:
        raise Exception(
            f"Unsupported data_type for Apple AppStore: {data_type}")

    logger.info("Assign task completed")


def _prepare_reddit_query_dict(url):
    ret_query_dict = dict()

    url_split = urlsplit(url)
    query_dict = parse_qs(url_split.query)
    subreddit_names = query_dict.get("sub_reddits", [])
    subreddit_str = ""
    if len(subreddit_names):
        subreddit_names_list = subreddit_names[0].split(",")
        print(subreddit_names_list)
        subreddit_str = "+".join(subreddit_names_list)

    ret_query_dict["subreddits"] = subreddit_str

    keywords = query_dict.get("keywords", [])
    keywords_str = ""
    if len(keywords):
        keywords_list = keywords[0].split(",")
        mapped_keywords_list = []
        for keyword in keywords_list:
            mapped_keywords_list.extend(
                [f"selftext:{keyword}", f"title:{keyword}"])
        keywords_str = " OR ".join(mapped_keywords_list)

    ret_query_dict["keywords"] = keywords_str

    return ret_query_dict


@auto_session
def generate_reddit_steps(batch: ETLCompanyBatch, session=None):
    today = dt.datetime.today()

    etl_step = utils.load_etl_step(batch, session=session)
    meta_data = batch.meta_data or {}
    data_type = meta_data.get("data_type")

    try:
        # Fetch total reviews
        url = batch.url
        query_dict = _prepare_reddit_query_dict(url)

        user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
        reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=user_agent,
        )

        params = {"sort": "new", "limit": None}
        submissions = reddit.subreddit(query_dict["subreddits"]).search(
            query_dict["keywords"], **params
        )
        total = len(list(submissions))

        paging_info = meta_data.get("paging_info", {})
        by_langs = paging_info.get("by_langs", {})
        by_langs["all"] = total
        paging_info["by_langs"] = by_langs
        paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
        meta_data["paging_info"] = paging_info
        batch.meta_data = meta_data
        session.commit()
        logger.info(
            f"Dispatching Reddit crawl task, data_type: {data_type}, url {url}")
        if data_type == "review_stats":
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = (
                f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
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
                "version": "1.0",
                "data_type": "review_stats",
                "url": url,
                "total_submissions": total
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
            pages = math.ceil(total / config.REDDIT_PAGE_SIZE) or 1
            steps = math.ceil(total / config.ROWS_PER_STEP) or 1
            paging = math.ceil(config.ROWS_PER_STEP / config.REDDIT_PAGE_SIZE)

            logger.info(f"Generating step details, there are {steps} steps")
            publisher = pubsub_v1.PublisherClient()
            project_id = config.GCP_PROJECT
            topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
            topic_path = publisher.topic_path(project_id, topic_id)

            step_detail_list = []
            for step in range(steps):
                start = step * paging + 1
                end = (
                    list(range(pages + 1)[start: start + paging]) or [1])[-1]
                step_detail = ETLStepDetail()
                step_detail.request_id = batch.request_id
                step_detail.step_detail_name = f"{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip(
                )
                step_detail.status = "waiting"
                step_detail.step_id = etl_step.step_id
                step_detail.batch_id = batch.batch_id
                step_detail.file_id = step + 1
                step_detail.paging = f"{start}:{end}"
                step_detail.created_at = today
                step_detail.updated_at = today
                step_detail.progress_current = 0
                step_detail.progress_total = (end + 1) - start
                step_detail.lang = "en"
                step_detail.meta_data = {
                    "version": "1.0",
                    "data_type": "review",
                    "url": url,
                    "total_submissions": total
                }
                session.add(step_detail)
                session.commit()
                step_detail_list.append(step_detail.step_detail_id)

            payload = json.dumps(
                {"url": url, "batch_id": batch.batch_id, "ids": step_detail_list}
            )
            payload = payload.encode("utf-8")
            logger.info(f"Publishing a message to {topic_id}: {payload}")
            future = publisher.publish(topic_path, payload)
            logger.info(
                f"Published message to {topic_id}, result: {future.result()}")

        else:
            raise Exception(f"Unsupported data_type for Reddit: {data_type}")

        logger.info("Assign task completed")

    except Exception as error:
        logger.exception(f"Can not fetch reddit reviews - error: {error}")
        raise error


@auto_session
def generate_coresignal_linkedin_steps(batch: ETLCompanyBatch, session=None):
    request = session.query(ETLRequest).get(batch.request_id)
    company_website_url = request.payload.get("company_website_url")
    data_type = batch.meta_data.get("data_type")
    if data_type == "coresignal_stats":
        coresignal_linkedin.generate_stats_step(batch, company_website_url, session)
    elif data_type == "coresignal_employees":
        coresignal_linkedin.generate_employee_steps(batch, company_website_url, session)


@auto_session
def has_running_crawl_step(batch: ETLCompanyBatch, session=None):
    assert batch is not None, "Requires batch to check"
    assert session is not None, "Need database session"
    logger.info(f"has_running_crawl_step: batch {batch.batch_name}...")

    step = (
        session.query(ETLStep)
        .filter(ETLStep.batch_id == batch.batch_id)
        .filter(ETLStep.step_name.like("crawl%"))
        .first()
    )
    steps = (
        session.query(ETLStepDetail).filter(
            ETLStepDetail.step_id == step.step_id).all()
        if step
        else []
    )
    logger.info(
        f"There are {len(steps)} step details in step: {step.step_id if step else None}"
    )

    has_running = False
    if len(steps) > 0:
        has_running = True

    logger.info(f"Batch {batch.batch_name} has running step? {has_running}")
    return has_running


@auto_session
def check_and_update_step_status(batch: ETLCompanyBatch, session=None):
    assert batch is not None, "Requires batch to check"
    assert session is not None, "Need database session"

    etl_step = (
        session.query(ETLStep)
        .filter(ETLStep.batch_id == batch.batch_id)
        .filter(ETLStep.step_name.like("crawl%"))
        .first()
    )
    logger.info(
        f"Checking status for crawling step: {etl_step.step_id}, {etl_step.step_name} ..."
    )

    etl_step_details = (
        session.query(ETLStepDetail)
        .filter(ETLStepDetail.step_id == etl_step.step_id)
        .all()
        if etl_step
        else []
    )
    logger.info(
        f"There are {len(etl_step_details)} step_details for step: {etl_step.step_id}, computing status for this step ..."
    )

    status = etl_step.status
    if len(etl_step_details) == 0:
        status = "waiting"
    elif all(list(map(lambda it: it.status == "finished", etl_step_details))):
        status = "finished"
    elif all(
        list(
            map(
                lambda it: it.status in ["completed with error", "finished"],
                etl_step_details,
            )
        )
    ):
        status = "completed with error"
    elif any(list(map(lambda it: it.status == "running", etl_step_details))):
        status = "running"

    logger.info(f"Status for step {etl_step.step_id}: {status}")
    etl_step.status = status
    etl_step.updated_at = dt.datetime.today()
    session.commit()


def publish_dispatcher_error_details(
    batch: ETLCompanyBatch, error_time, exception, additional_info=None, logger=None
):
    """
    Construct a valid payload and publish to error topic
    """
    # get necessary info
    data_type = batch.meta_data.get("data_type")
    data_version = batch.data_version
    info_dict = additional_info if additional_info else {}

    publish_error_message(
        project_id=config.GCP_PROJECT,
        topic_id=config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR,
        company_datasource_id=batch.company_datasource_id,
        batch_id=batch.batch_id,
        step_detail_id=None,
        company_id=batch.company_id,
        company_name=batch.company_name,
        source_id=batch.source_id,
        source_name=batch.source_name,
        data_type=data_type,
        error_source="dispatcher",
        error_code="FAILED",
        severity="CRITICAL",
        exception=exception,
        total_step_details=None,
        error_time=error_time,
        data_version=data_version,
        additional_info=info_dict,
        logger=logger,
    )
