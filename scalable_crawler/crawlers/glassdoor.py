import math
import os
import re
import json
import requests
import js2py
import pandas as pd
import datetime as dt

from retry import retry
from google.cloud import storage, pubsub_v1
from urllib.parse import urlparse
from datetime import date, timedelta
from bs4 import BeautifulSoup
from typing import Callable, List, Tuple, Optional

import utils
import config
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from helpers import log_crawl_stats

logger = utils.load_logger()

STRATEGY_GLASSDOOR = "glassdoor"
STRATEGY_LUMINATI = "luminati"
STRATEGY_WEBWRAPPER = "webwrapper"
STRATEGY_DATASHAKE = "datashake"

GLASSDOOR_URL = "https://www.glassdoor.com"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"
DELIMITER = "\t"

JOB_TYPE_MAPS = {
    "search-jobs.job-type-options.all": "All Job Types",
    "search-jobs.job-type-options.fulltime": "Full-time",
    "search-jobs.job-type-options.parttime": "Part-time",
    "search-jobs.job-type-options.contract": "Contract",
    "search-jobs.job-type-options.internship": "Internship",
    "search-jobs.job-type-options.temporary": "Temporary",
    "search-jobs.job-type-options.apprenticeship": "Apprentice/Trainee",
    "search-jobs.job-type-options.entrylevel": "Entry Level",
    "search-jobs.job-type-options.trainee": "Trainee",
}


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def _extract_app_cache(content: str) -> dict:
    app_cache = {}
    soup = BeautifulSoup(content, "html.parser")
    for elm in soup.select("script"):
        content = elm.decode_contents().strip()
        if content.startswith("window.appCache"):
            js_wrapper: js2py.base.JsObjectWrapper = js2py.eval_js(content)
            app_cache = js_wrapper.to_dict()
            return app_cache

    return app_cache


def _select_text(element):
    if element:
        return element.text
    else:
        return ""


def _build_page_url(url, page):
    return url.replace(".htm", f"_P{page}.htm")


def _get_reviews_script(content: str) -> str:
    """
    Return inner text of script tag that contains reviews data.
    """
    script_content = ""
    soup = BeautifulSoup(content, "html.parser")
    for elm in soup.select("script"):
        elm_inner_text = elm.decode_contents()
        if "window.appCache" in elm_inner_text:
            script_content = elm_inner_text
            break

    return script_content


def _is_reviews_page_valid(content: str) -> bool:
    return len(_get_reviews_script(content)) > 0


def _is_jobs_page_valid(content: str) -> bool:
    app_cache = _extract_app_cache(content=content)
    extracted_data: dict = app_cache["initialState"]["extractedData"]

    check_keys = ["selectedCountry", "totalJobsCount", "countryCounts", "jobListings"]
    if any([k not in extracted_data for k in check_keys]):
        return False

    total_jobs_count = extracted_data["totalJobsCount"]
    selected_country_count = extracted_data["countryCounts"][
        str(extracted_data["selectedCountry"])
    ]
    if total_jobs_count != selected_country_count:
        return False

    return True


def _find_open_close_pair(marks: List[int]) -> Optional[Tuple[int, int]]:
    for i in range(len(marks) - 1):
        if marks[i] > 0 and marks[i + 1] < 0:
            return (marks[i], marks[i + 1])
    return None


def _extract_js_object(js_content: str, keys: List[str]) -> Optional[str]:
    """
    TODO: Complete this function to use all provided keys.
    """
    marks = []
    for i, c in enumerate(js_content):
        if c == "{":
            marks.append(i)
        elif c == "}":
            marks.append(i * -1)

    open_marks = [i for i in marks if i > 0]
    close_marks = [i for i in marks if i < 0]

    assert len(open_marks) == len(close_marks), "Invalid JS script"

    pairs = []
    pair = _find_open_close_pair(marks=marks)
    while pair is not None:
        pairs.append(pair)
        marks.remove(pair[0])
        marks.remove(pair[1])
        pair = _find_open_close_pair(marks=marks)

    key_idx = js_content.index(keys[-1])
    open_idx = 0
    for i in open_marks:
        if i > key_idx:
            open_idx = i
            break

    for pair in pairs:
        if pair[0] == open_idx:
            return js_content[pair[0]: pair[1] * -1 + 1]

    return None


@retry(tries=6, delay=1, backoff=3)
def request_get(check_fn: Callable[[str], bool] = None, *args, **kwargs):
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
        raise ValueError(f"(status_code={res.status_code}, text={res.text[:100]})")
    elif check_fn != None:
        assert check_fn(res.text), "Unexpected content!"
    return res


@retry(tries=3, delay=1, backoff=2)
def get_soup_for_overview(*args, **kwargs):
    """
    Check if Glassdoor overview module available. Retry if not.
    Overview module contains all relevant info to be scraped.
    """
    response = request_get(*args, **kwargs)
    soup = BeautifulSoup(response.content, "html.parser")
    if soup.select_one('div[data-test="employerOverviewModule"]'):
        return soup
    else:
        logger.warning("GLASSDOOR OVERVIEW - Overview module not found, retrying...")
        raise Exception("GLASSDOOR OVERVIEW - Overview module not available")


@auto_session
def _crawl_reviews(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"GLASSDOOR: Crawling glassdoor reviews on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    lang = step_detail.lang.strip()
    params = (
        ("filter.iso3Language", lang),
        ("filter.employmentStatus", "REGULAR"),
        ("filter.employmentStatus", "PART_TIME"),
    )

    def _extract_item(review):
        try:
            review["title"] = review["summary"]
            review["review_id"] = review["reviewId"]
            review["rating"] = review["ratingOverall"]
            review["date"] = review["reviewDateTime"]

            return review
        except:
            logger.exception(f"GLASSDOOR: Unable to extract: {review}")
            return None

    total_reviews = []
    page_exceptions = []
    error_pages = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
    for index, page in enumerate(pages):
        # Publish progress
        logger.info(
            f"GLASSDOOR: Publishing crawling step detail {step_detail.step_detail_id} progress..."
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
            f"GLASSDOOR: Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%"
        )

        # Crawl
        page_url = _build_page_url(url, page)
        logger.info(f"GLASSDOOR: Crawling {page} with url: {page_url}")

        res = None
        try:
            results = []
            res = request_get(
                check_fn=_is_reviews_page_valid,
                url=page_url,
                params=params,
                verify=False,
            )
            logger.info(
                f"GLASSDOOR calling to {page_url} and status code : {res.status_code}"
            )
            script_content = _get_reviews_script(content=res.text)
            script_content = _extract_js_object(
                script_content, ["window", "appCache", "apolloState"]
            )
            script_obj: dict = json.loads(script_content)
            root_query = script_obj.get("ROOT_QUERY", {})
            employee_reviews_key = ""
            for key in root_query.keys():
                if key.startswith("employerReviews") and ("reviews" in root_query[key]):
                    employee_reviews_key = key
                    break
            reviews = root_query.get(employee_reviews_key, {}).get("reviews", [])
            results = list(map(_extract_item, reviews))
            results = list(filter(lambda item: item is not None, results))

        except Exception as e:
            logger.warning(
                f"GLASSDOOR: Unable to crawl page {page} with url: {page_url}"
            )
            logger.warning(f"GLASSDOOR error: {e}")
            page_exceptions.append(e)
            error_pages.append(page)
            continue

        total_reviews = [*total_reviews, *results]
        logger.info(f"GLASSDOOR: Crawled {len(total_reviews)} review items")

    # update DB for progress
    step_detail.item_count = len(total_reviews)
    session.commit()

    df = pd.DataFrame(total_reviews)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    # For fixing surrogates not allowed
    with open(output_file, "w", errors="replace") as _file:
        df.to_csv(_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded review file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(batch=batch, step_detail=step_detail, item_count=len(total_reviews))

    return page_exceptions, total_pages, error_pages


def _extract_apollo_cache_data(content):
    soup = BeautifulSoup(content, "html.parser")
    scripts = soup.find("script", {"id": "__NEXT_DATA__"})
    script_decoded = json.loads(scripts.decode_contents().strip())
    temp_data = script_decoded["props"]["pageProps"]["apolloCache"]["ROOT_QUERY"]
    keys = list(temp_data.keys())

    # extract job list
    job_lists = temp_data[keys[10]]["jobListings"]

    # extract pagination links
    pagination_links = temp_data[keys[10]]["paginationLinks"]

    # extract total job count
    total_job_counts = temp_data[keys[10]]["totalJobsCount"]

    return job_lists, pagination_links, total_job_counts


@auto_session
def _crawl_jobs(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"GLASSDOOR: Crawling glassdoor jobs on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    # NOTE: Expecting `step_detail.paging` like '1:%'
    PAGE_SIZE = 40
    page_exceptions = []
    error_pages = []
    cookies = {}
    total_jobs = []
    lst_job_ids = []
    total_jobs_count = 0
    total_pages_count = 0
    lang = str(step_detail.lang).strip()
    job_page_url = (
        urlparse(str(url))._replace(query=f"filter.countryId={lang}").geturl()
    )
    current_page = 1
    while True:
        try:
            res = request_get(
                url=job_page_url,
                check_fn=_is_jobs_page_valid,
                cookies=cookies,
                verify=False,
            )
        except Exception as e:
            logger.exception(
                f"GLASSDOOR: Error happens while crawling job page {current_page}: {job_page_url}, params: filter.countryId={step_detail.lang}"
            )
            page_exceptions.append(e)
            error_pages.append(current_page)

            # Continue with the next page
            if current_page == 1:
                job_page_url = job_page_url.replace(".htm", "_P2.htm")
            else:
                job_page_url = job_page_url.replace(
                    f"_P{current_page}.htm", f"_P{current_page + 1}.htm"
                )
            current_page += 1
            continue

        extracted_data: dict = {}
        job_listings: List[dict] = []
        pagination_links: List[dict] = []

        # Extract data for current page
        job_listings, pagination_links, total_jobs_count = _extract_apollo_cache_data(content=res.text)
        total_pages_count = int(total_jobs_count / PAGE_SIZE) + 1

        # If no job returned, stop crawling
        if not job_listings:
            break

        # Extract jobs
        current_jobs = []
        for job in job_listings:
            jobview: dict = job["jobview"]
            age_in_days = int(jobview["header"]["ageInDays"])
            date_posted = (date.today() - timedelta(days=age_in_days)).isoformat()
            item = {
                "job_id": jobview["job"]["listingId"],
                "title": jobview["job"]["jobTitleText"],
                "location": jobview["header"]["locationName"],
                "time": date_posted,
                "job_type": "",
                "job_function": jobview["header"]["goc"],
                "country": str(jobview["header"]["jobCountryId"]),
                "posted": date_posted,
                "job_link": jobview["header"]["jobLink"],
            }

            if item["job_id"] not in lst_job_ids:
                lst_job_ids.append(item["job_id"])
                current_jobs.append(item)

        total_jobs.extend(current_jobs)

        # Preparing before navigate to the next page
        if current_page > math.ceil(total_jobs_count / PAGE_SIZE) + total_pages_count:
            # Reached the end
            break
        else:
            # Update `job_page_url` to the next page, update cookies.
            next_page_url = ""
            next_pages = [
                item
                for item in pagination_links
                if item["pageNumber"] == current_page + 1
            ]  # `pagination_links` is sorted by `pageNumber` so does `next_pages`

            if next_pages:
                # Use the provided URL
                next_page_url = str(next_pages[0]["urlLink"])
            else:
                # Build URL
                if current_page == 1:
                    next_page_url = job_page_url.replace(".htm", "_P2.htm")
                else:
                    next_page_url = job_page_url.replace(
                        f"_P{current_page}.htm", f"_P{current_page + 1}.htm"
                    )

            if next_page_url.startswith("/"):
                parts = urlparse(job_page_url)
                next_page_url = f"{parts.scheme}://{parts.netloc}{next_page_url}"

            current_page += 1
            job_page_url = next_page_url
            cookies = res.cookies.get_dict()

        # Publish progress
        logger.info(
            f"GLASSDOOR: Publishing crawling step detail {step_detail.step_detail_id} progress..."
        )
        data = {
            "type": "crawl",
            "event": "progress",
            "batch": batch.to_json(),
            "step": step.to_json(),
            "step_detail": step_detail.to_json(),
            "progress": {"total": total_pages_count, "current": current_page},
        }
        data = json.dumps(data)
        data = data.encode("utf-8")
        utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_PROGRESS, data, logger=logger)
        logger.info(
            f"GLASSDOOR: Published crawl progress {(current_page / total_pages_count) * 100:.2f}%"
        )

    # update DB for progress
    step_detail.item_count = len(total_jobs)
    session.commit()

    # save csvs locally
    df = pd.DataFrame(total_jobs)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded job file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(batch=batch, step_detail=step_detail, item_count=len(total_jobs))

    return page_exceptions, total_pages_count, error_pages


@auto_session
def _crawl_overview(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"GLASSDOOR: Crawling glassdoor overview on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    page_exceptions = []
    error_pages = []
    try:
        soup = get_soup_for_overview(url=url)

        info = {
            "website": _select_text(soup.select_one('a[data-test="employer-website"]')),
            "headquarters": _select_text(
                soup.select_one('div[data-test="employer-headquarters"]')
            ),
            "size": _select_text(soup.select_one('div[data-test="employer-size"]')),
            "founded": _select_text(
                soup.select_one('div[data-test="employer-founded"]')
            ),
            "revenue": _select_text(
                soup.select_one('div[data-test="employer-revenue"]')
            ),
            "industry": _select_text(
                soup.select_one('div[data-test="employer-industry"]')
            ),
            "description": _select_text(
                soup.select_one('span[data-test="employerDescription"]')
            ),
        }

        # update DB for progress
        step_detail.item_count = 1
        session.commit()

        # save locally
        df = pd.DataFrame([info])
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(output_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = output_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        logger.info(f"Uploaded overview file to gcs: {dst_file}")

    except Exception as e:
        logger.exception()
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, 1, error_pages


@auto_session
def _crawl_review_stats(
        url, step_detail, batch=None, step=None, session=None, **kwargs
):
    def _extract_reviews_count(soup):
        pagination_text = _select_text(soup.select_one(".paginationFooter"))
        match = re.search(r"of ([0-9,]+).*Reviews", pagination_text)
        count = int(match.group(1).replace(",", "") if match and match.group(1) else 1)
        return count

    def _extract_average_rating(soup):
        average_rating_text = _select_text(
            soup.select_one(
                ".v2__EIReviewsRatingsStylesV2__ratingNum.v2__EIReviewsRatingsStylesV2__large"
            )
        )
        average_rating = float(average_rating_text) if average_rating_text else None
        return average_rating

    logger.info(
        f"GLASSDOOR: Crawling glassdoor review_stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    lang = "eng"
    params = (
        ("filter.iso3Language", lang),
        ("filter.employmentStatus", "REGULAR"),
        ("filter.employmentStatus", "PART_TIME"),
    )

    page_exceptions = []
    error_pages = []
    total_pages = 1
    try:
        response = request_get(url=url, params=params, verify=False)
        soup = BeautifulSoup(response.text, features="lxml")
        review_stats_data = {}
        review_stats_data["total_reviews"] = _extract_reviews_count(soup)
        review_stats_data["average_rating"] = _extract_average_rating(soup)

        # update DB for progress
        step_detail.item_count = 1
        session.commit()

        # save locally
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
            item_count=review_stats_data["total_reviews"],
        )

    except Exception as e:
        logger.exception(
            f"GLASSDOOR REVIEW_STATS: Unable to crawl review_stats url: {url}"
        )
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def _glassdoor_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
    meta_data = step_detail.meta_data or {}
    if meta_data.get("data_type") == "review_stats":
        # handle crawl reviews
        return _crawl_review_stats(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "review":
        # handle crawl reviews
        return _crawl_reviews(
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


def _get_crawler(strategy=STRATEGY_GLASSDOOR):
    logger.info(f"GLASSDOOR: Use crawler strategy {strategy}")
    if strategy == STRATEGY_GLASSDOOR:
        return _glassdoor_scraper
    else:
        raise Exception(f"GLASSDOOR: Unsupported strategy: {strategy}")


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
                f"GLASSDOOR: Publishing crawled step detail {step_detail.step_detail_id} to Pub/Sub..."
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
            logger.info(f"GLASSDOOR: Message published to Pub/Sub {result}")

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
                f"GLASSDOOR: Unable to crawl step_detail: {step_detail_id} - url: {url}"
            )
            data = {
                "type": "crawl",
                "event": "fail",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
                "error": f"Something went wrong: {str(e)}",
            }
            data = json.dumps(data)
            data = data.encode("utf-8")
            # create a new publisher everytime a message is to be published
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_AFTER_TASK
            )
            logger.info(
                f"GLASSDOOR: Publish message to topic: {topic_path} - payload: {data}"
            )
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"GLASSDOOR: Message published to Pub/Sub {result}")

            # utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_AFTER_TASK, data, logger=logger)


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
