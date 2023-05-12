import math
import datetime as dt
import json
import re

from bs4 import BeautifulSoup

import config
import utils
from database import (
    ETLCompanyBatch,
    ETLStepDetail,
    auto_session,
)
from jobstats import JobStats

STRATEGY_AMBITIONBOX = "ambitionbox"
base_url = "https://www.ambitionbox.com/api/v2/jobs/company/{company_id}?excludeJobId=123{page}"


logger = utils.load_logger()

headers = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
}

options = {
    "headers": {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36"
    }
}

if config.LUMINATI_HTTP_PROXY:
    options["proxies"] = {
        "http": config.LUMINATI_HTTP_PROXY,
        "https": config.LUMINATI_HTTP_PROXY,
    }


@auto_session
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_AMBITIONBOX,
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

    pages = math.ceil(total / config.AMBITIONBOX_PAGE_SIZE) or 1
    steps = math.ceil(total / config.ROWS_PER_STEP) or 1
    paging = math.ceil(config.ROWS_PER_STEP / config.AMBITIONBOX_PAGE_SIZE)

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
def dispatch_ambitionbox(batch: ETLCompanyBatch, session=None, **kwargs):
    today = dt.datetime.today()
    options = {"headers": headers}

    if config.LUMINATI_HTTP_PROXY:
        options["proxies"] = {
            "http": config.LUMINATI_HTTP_PROXY,
            "https": config.LUMINATI_HTTP_PROXY,
        }

    # handle
    url = batch.url
    logger.info(f"Dispatching url: {url}")
    meta_data = batch.meta_data
    data_type = meta_data.get("data_type")

    if data_type == "review_stats":
        url = batch.url
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
            "strategy": STRATEGY_AMBITIONBOX,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "overview":
        url = batch.url
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
            "url": url,
            "lang": "en",
        }
        session.add(step_detail)
        session.commit()

        payload = {
            "batch_id": batch.batch_id,
            "url": url,
            "strategy": STRATEGY_AMBITIONBOX,
            "ids": [step_detail.step_detail_id],
        }
        payload = json.dumps(payload)
        utils.publish_pubsub(
            config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
        )

    elif data_type == "review":
        url = batch.url

        def get_total_review(soup):
            total_review_string = (
                soup.find("div", {"class": "reviews-info"})
                .find("h2")
                .text.replace("\\n", "")
                .strip()
            )
            total_review_num = total_review_string.split()[0]
            try:
                return int(total_review_num)
            except ValueError:
                abbr_vals = {"k": 3, "m": 6, "b": 9, "t": 12}
                return int(
                    float(total_review_num[:-1]) * 10 ** abbr_vals[total_review_num[-1]]
                )

        response = utils.request_get(
            url=url, logger=logger, unblocker=True, valid_status_codes=[200], verify=False
        )
        soup = BeautifulSoup(response.content)

        total_review = get_total_review(soup)
        dispatch_tasks(
            total_review, batch, session=session, data_type="review", lang="en", url=url
        )

    else:
        # Dispatch crawl jobs
        url = batch.url

        def extract_url(url):
            pattern = r"https://www.ambitionbox.com/jobs/([a-zA-Z0-9_-]+)-jobs"
            search = re.search(pattern, url)
            company_job = search.group(1)

            url_crawl_job = f"https://www.ambitionbox.com/api/v2/search?query={company_job}&category=all&type=companies"
            response = utils.request_get(
                url=url_crawl_job,
                logger=logger,
                unblocker=True,
                valid_status_codes=[200],
                timeout=20,
                verify=False
            )
            soup = BeautifulSoup(response.content, "html.parser")
            if soup.find("pre"):
                logger.info("dispatcher ambitionbox job html response")
                content = soup.find("pre").find(text=True)
                data_dict = json.loads(content)
            else:
                logger.info("dispatcher ambitionbox job dict response")
                content = response.content
                data_dict = json.loads(content)
            company_id = data_dict["data"][0].get("companyId")
            return company_id

        company_id = extract_url(url)
        first_page_param = f"&page=1"
        overall_url = base_url.format(company_id=company_id, page=first_page_param)
        response = utils.request_get(
            url=overall_url,
            logger=logger,
            unblocker=True,
            valid_status_codes=[200],
            timeout=20,
            verify=False
        )
        soup = BeautifulSoup(response.content, "html.parser")
        if soup.find("pre"):
            logger.info("dispatcher ambitionbox job first page data html response")
            content = soup.find("pre").find(text=True)
            first_page_data = json.loads(content)
        else:
            logger.info("dispatcher ambitionbox job first page data dict response")
            content = response.content
            first_page_data = json.loads(content)
        # first_page_data = response.json()
        if response.status_code == 200:
            total_page_job = first_page_data["data"]["Pagination"].get("TotalPages")
        else:
            raise Exception("[Ambitionbox job] Cannot get total pages")
        job_count = total_page_job * config.AMBITIONBOX_PAGE_SIZE

        dispatch_tasks(
            job_count,
            batch,
            strategy=STRATEGY_AMBITIONBOX,
            session=session,
            data_type="job",
            lang="en",
            url=url,
            start_id=1,
        )

        # Log crawl statistics to BigQuery
        n_jobs = 0
        job_stats = JobStats(proxy_url=config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY)
        try:
            n_jobs = job_stats.get_ambitionbox_job_count(url=batch.url)
            job_stats.log_crawl_stats(batch=batch, item_count=n_jobs)
        except:
            pass


def get_scraper(strategy=STRATEGY_AMBITIONBOX):
    if strategy == STRATEGY_AMBITIONBOX:
        return dispatch_ambitionbox
    else:
        raise Exception(f"Unsupported strategy {strategy}")
