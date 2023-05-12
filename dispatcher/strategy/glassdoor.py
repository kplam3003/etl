import math
import datetime as dt
import re
import json
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from retry import retry
import js2py

import config
import utils
from database import (
    ETLCompanyBatch,
    ETLStepDetail,
    auto_session,
)
from jobstats import JobStats

STRATEGY_GLASSDOOR = "glassdoor"

GLASSDOOR_URL = "https://www.glassdoor.com"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"
logger = utils.load_logger()


headers = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
}


@retry(tries=5, delay=1, jitter=2)
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
        logger.warning(f"Can't get Glassdoor html from this url {res.url}")
        logger.warning(f"Retrying....")
        logger.exception(f"Error {res.status_code}, response:{res.text[:100]}")
        raise Exception(f"Error {res.status_code}, response:{res.text[:100]}")
    return res


def _select_text(element):
    if element:
        return element.text
    else:
        return ""


def _extract_langs(soup):
    lang_options = soup.select(
        "select[data-test='ContentFiltersLanguageDropdown'] > option"
    )
    lang_ids = list(map(lambda option: option.attrs.get("value"), lang_options))
    return lang_ids


def _extract_reviews_count(soup):
    pagination_text = _select_text(soup.select_one(".paginationFooter"))
    match = re.search(r"of ([0-9,]+).*Reviews", pagination_text)
    count = int(match.group(1).replace(",", "") if match and match.group(1) else 1)
    return count


@auto_session
def dispatch_job_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_GLASSDOOR,
    session=None,
    data_type="job",
    start_id=1,
    lang="eng",
    url=None,
    country=None,
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

    page_size = (
        config.GLASSDOOR_REVIEW_PAGE_SIZE
        if data_type == "review"
        else config.GLASSDOOR_JOB_PAGE_SIZE
    )

    pages = math.ceil(total / page_size) or 1
    paging = math.ceil(config.ROWS_PER_STEP / page_size)
    etl_step = utils.load_etl_step(batch, session=session)
    logger.info(f"Generating step details, there are 1 steps")
    start = 1
    end = pages
    step_detail = ETLStepDetail()
    step_detail.request_id = batch.request_id
    step_detail.step_detail_name = f"{data_type}-{etl_step.step_name}-{today.strftime('%Y%m%d')}-{lang}-f{start_id + 1:03d}".strip()
    step_detail.status = "waiting"
    step_detail.step_id = etl_step.step_id
    step_detail.batch_id = batch.batch_id
    step_detail.file_id = start_id + 1
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
        "country": country,
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
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_GLASSDOOR,
    session=None,
    data_type="review",
    start_id=1,
    lang="eng",
    url=None,
    country=None,
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

    page_size = (
        config.GLASSDOOR_REVIEW_PAGE_SIZE
        if data_type == "review"
        else config.GLASSDOOR_JOB_PAGE_SIZE
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
            "country": country,
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
def glassdoor_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    today = dt.datetime.today()
    url = batch.url
    parsed = urlparse(url)
    path = urlparse(url).path
    url = urljoin(GLASSDOOR_URL, parsed.path)
    global_url = urljoin(GLASSDOOR_URL, path)
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type") if meta_data else "review"
    etl_step = utils.load_etl_step(batch, session=session)

    logger.info(
        f"Dispatching Glassdoor crawl task, data_type: {data_type}, url {global_url}"
    )
    if data_type == "review_stats":
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
            "url": global_url,
            "lang": "eng",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": global_url,
            "ids": [step_detail.step_detail_id],
            "strategy": STRATEGY_GLASSDOOR,
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "overview":
        # 1. Dispatch crawl company overview
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
        step_detail.lang = "eng"
        step_detail.meta_data = {
            "version": "1.0",
            "data_type": "overview",
            "url": global_url,
            "lang": "eng",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": global_url,
            "strategy": STRATEGY_GLASSDOOR,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "review":
        # 2. Dispatch crawl reviews
        # dispatch for english
        lang = "eng"
        params = (
            ("filter.iso3Language", lang),
            ("filter.employmentStatus", "REGULAR"),
            ("filter.employmentStatus", "PART_TIME")
        )
        res = request_get(global_url, params=params, verify=False)
        logger.info(f"Finding reviews on url: {res.url}")
        soup = BeautifulSoup(res.text, features="lxml")
        langs = _extract_langs(soup)
        reviews_count = _extract_reviews_count(soup)
        dispatch_tasks(
            reviews_count,
            batch,
            session=session,
            data_type="review",
            lang=lang,
            url=global_url,
        )
        start_id = math.ceil(reviews_count / config.ROWS_PER_STEP) + 1

        # dispatch in other supported langs
        for lang in filter(lambda l: l != "eng", langs):
            params = (
                ("filter.iso3Language", lang),
                ("filter.employmentStatus", "REGULAR"),
                ("filter.employmentStatus", "PART_TIME")
            )
            res = request_get(global_url, params=params, verify=False)
            logger.info(f"Finding reviews on url: {global_url}")
            soup = BeautifulSoup(res.text, features="lxml")
            reviews_count = _extract_reviews_count(soup)
            start_id += math.ceil(reviews_count / config.ROWS_PER_STEP)
            dispatch_tasks(
                reviews_count,
                batch,
                session=session,
                data_type="review",
                lang=lang,
                url=global_url,
                start_id=start_id,
            )

    elif data_type == "job":
        # 3. Dispatch crawl jobs
        params = (("filter.countryId", 1),)
        res = request_get(global_url, params=params, verify=False)
        logger.info(f"Finding jobs on url: {global_url}")
        soup = BeautifulSoup(res.text, "html.parser")
        try:
            # job count
            job_count_text = None
            item = soup.find("a", {
                "class": "activePage d-flex flex-column justify-content-end align-items-center pb-xsm css-mc0zii e1lua58c1"})
            job_count = item.find("div", {"class": "count"})
            if job_count is not None:
                job_count_text = int(job_count.text)

            # countries
            script = soup.select("script:-soup-contains('countryMenu')")
            if script is not None:
                text = script[0].string.replace("\\", "").split(";")
                footer_data = text[5]
                if not footer_data:
                    logger.exception(
                        f"Unable to retrieve jobs information on url: {global_url}"
                    )
                footer_data = footer_data[(footer_data.index("countryMenu") - 1):]
                footer_data = footer_data[(footer_data.index("childNavigationLinks") - 1):]
                open_bracket = footer_data.index("[")
                close_bracket = footer_data.index("]")
                footer_data = footer_data[open_bracket: (close_bracket + 1)]
                countries = eval(footer_data)
                for country in countries:
                    country_name = country.get("textKey")
                    country_id = country.get("id")
                    logger.info(f"Found {job_count_text} job records on url: {res.url}, country name: {country_name} with id: {country_id}")
                    dispatch_job_tasks(
                        job_count_text,
                        batch,
                        session=session,
                        data_type="job",
                        lang=country_id,
                        url=global_url,
                        country=country_name,
                    )
        except Exception as error:
            logger.exception(
                f"Unable to retrieve jobs information on url: {global_url}"
            )
            raise error

        # Log crawl statistics to BigQuery
        n_jobs = 0
        job_stats = JobStats(proxy_url=config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY)
        try:
            n_jobs = job_stats.get_glassdoor_job_count(url=batch.url)
            job_stats.log_crawl_stats(batch=batch, item_count=n_jobs)
        except:
            pass

    else:
        raise Exception(f"Unsupported Glassdoor data_type: {data_type}")


def get_scraper(strategy=STRATEGY_GLASSDOOR):
    if strategy == STRATEGY_GLASSDOOR:
        return glassdoor_scrape
    else:
        raise Exception(f"Not supported strategy: {strategy}")
