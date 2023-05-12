import re
import json
import os
import js2py
import datetime as dt
import pandas as pd

from typing import List
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from google.cloud import pubsub_v1, storage

import utils
import config

from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from utils import publish_step_detail_error
from helpers import log_crawl_stats

logger = utils.load_logger()


STRATEGY_AMBITIONBOX = "ambitionbox"
DELIMITER = "\t"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15"
}

if config.LUMINATI_HTTP_PROXY:
    proxies = {
        "http": config.LUMINATI_HTTP_PROXY,
        "https": config.LUMINATI_HTTP_PROXY,
    }
else:
    proxies = None


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def extract_reviews_in_page(soup):
    all_reviews = soup.find_all("div", {"itemprop": "review"})
    total_reviews = (
        soup.find("div", {"class": "reviews-info"})
        .find("h2")
        .text.replace("\\n", "")
        .strip()
    )
    total_review = total_reviews.split()[0]
    return all_reviews, total_review


@auto_session
def _crawl_review_stats(
    url, step_detail, batch=None, step=None, session=None, **kwargs
):
    def _crawl_review_stats_json(company_reviews_url: str) -> dict:
        parse_results = urlparse(company_reviews_url)
        company_name = parse_results.path.split("/")[2]
        if company_name.endswith("-reviews"):
            company_name = company_name[: len("-reviews") * -1]

        # Search company by name & find its ID
        search_res = utils.request_get(
            url=f"https://www.ambitionbox.com/api/v2/search?query={company_name}&category=all&type=companies",
            headers={"User-Agent": USER_AGENT},
            logger=logger,
            unblocker=True,
            verify=False,
            valid_status_codes=[200]
        )

        soup = BeautifulSoup(search_res.content, "html.parser")

        if soup.find("pre"):
            content = soup.find("pre").find(text=True)
            rating_dict = json.loads(content)
        else:
            content = search_res.content
            rating_dict = json.loads(content)

        company_id = [
            int(item["companyId"])
            for item in rating_dict.get("data")
            if item["url"] == company_name
        ][0]

        # Get company review by company ID
        rating_res = utils.request_get(
            url=f"https://www.ambitionbox.com/api/v2/reviews/rating-distribution/{company_id}",
            headers={"User-Agent": USER_AGENT},
            logger=logger,
            unblocker=True,
            verify=False,
            valid_status_codes=[200]
        )
        rating_dict = json.loads(rating_res.content)
        rating_data = rating_dict.get("data")
        rating_distribution = {}
        avg_category_ratings = {}
        if rating_data["Distribution"]:
            rating_distribution = {
                f"{item['Rating']} start": int(item["Count"])
                for item in rating_data["Distribution"]
            }
        if rating_data["AvgRating"]:
            avg_category_ratings = {
                "Job Security": float(rating_data["AvgRating"]["AvgJobSecurityRating"]),
                "Company Culture": float(
                    rating_data["AvgRating"]["AvgCompanyCultureRating"]
                ),
                "Skill Development": float(
                    rating_data["AvgRating"]["AvgSkillDevelopmentRating"]
                ),
                "Work-Life Balance": float(
                    rating_data["AvgRating"]["AvgWorkLifeRating"]
                ),
                "Work Satisfaction": float(
                    rating_data["AvgRating"]["AvgWorkSatisfactionRating"]
                ),
                "Career Growth": float(
                    rating_data["AvgRating"]["AvgCareerGrowthRating"]
                ),
                "Salary & Benefits": float(
                    rating_data["AvgRating"]["AvgCompensationBenefitsRating"]
                ),
            }

        # Get reviews by company ID
        _ = f"https://www.ambitionbox.com/api/v2/reviews/data/{company_id}?page=1&limit=10"

        return {
            "total_reviews": int(rating_data["TotalCount"]),
            "average_rating": float(
                rating_data["AvgRating"]["AvgOverallCompanyRating"]
            ),
            **rating_distribution,
            **avg_category_ratings,
        }

    logger.info(
        f"Crawling ambitionbox review_stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    page_exceptions = []
    total_pages = 1
    error_pages = []
    try:
        review_stats = _crawl_review_stats_json(company_reviews_url=url)

        # update counts into DB for progress
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame([review_stats], index=[0])
        src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(src_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = src_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(src_file, dst_file)
        logger.info(f"Uploaded overview file to gcs: {dst_file}")

        # Log crawl statistics to BigQuery
        log_crawl_stats(
            batch=batch,
            step_detail=step_detail,
            item_count=review_stats["total_reviews"]
        )

    except Exception as e:
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def _crawl_reviews(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling ambitionbox reviews on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    total_reviews = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    page_exceptions = []
    total_pages = len(pages)
    error_pages = []
    for index, page in enumerate(pages):
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
        # create a new publisher everytime a message is to be published
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_PROGRESS
        )
        future = publisher.publish(topic_path, data)
        _ = future.result()
        logger.info(f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%")

        try:
            # do the scraping
            logger.info(f"Crawling page number: {page} with url: {url}")
            page_url = url.strip("/") + f"?page={page}"
            response = utils.request_get(
                url=page_url, logger=logger, unblocker=True, verify=False
            )

            # Get raw data from HTML
            js_data = None
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                for elm in soup.select("script"):
                    script_content = elm.decode_contents().strip()
                    if script_content.startswith("window.__NUXT__"):
                        script_res: js2py.base.JsObjectWrapper = js2py.eval_js(script_content)
                        js_data = script_res.to_dict()
                        break

            # Extract review data
            if js_data:
                reviews: List[dict] = js_data["data"][0]["reviewsData"]["data"]
                page_reviews = [
                    {
                        "review_id": item["Id"],
                        "user_name": item["UserName"],
                        "created_at": item["Created"],
                        "title": item["JobProfile"]["Name"],
                        "rating": item["OverallCompanyRating"],
                        "likes": item["LikesText"],
                        "dislikes": item["DisLikesText"],
                        "work_details": item["WorkDescriptionText"],
                        "_raw": json.dumps(item, ensure_ascii=False)
                    } for item in reviews
                ]
                total_reviews.extend(page_reviews)

        except Exception as e:
            logger.exception(
                f"Error happens while crawling review page {page}: {page_url}"
            )
            page_exceptions.append(e)
            error_pages.append(page)

    # update counts into DB for progress
    step_detail.item_count = len(total_reviews)
    session.commit()

    # Write to CSV to upload file
    df = pd.DataFrame(total_reviews)
    src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    with open(src_file, "w", errors="replace") as f:
        df.to_csv(f, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = src_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(src_file, dst_file)
    logger.info(f"Uploaded overview file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(total_reviews)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _crawl_overview(url, step_detail, batch=None, step=None, session=None, **kwargs):
    def extract_data_overview(soup):
        item = {}
        item["overview"] = {}
        if soup.find("a", {"data-filter-name": "chips_Company-Tags_undefined"}):
            size = soup.find(
                "a", {"data-filter-name": "chips_Company-Tags_undefined"}
            ).text
        else:
            size = None
        name = soup.find("div", {"class": "newHInfo__cHead"}).text
        rating = soup.find("span", {"class": "newHInfo__rating"}).text
        # get country
        if soup.find("div", {"class": "textItem__val aboutItem__value"}):
            country = soup.find("div", {"class": "textItem__val aboutItem__value"}).text.split(',')[-1].strip()
        else:
            country = None
        # get long_description
        if soup.find("div", {"itemprop": "description"}):
            long_description = (
                soup.find("div", {"itemprop": "description"})
                .text.replace("\t", "")
                .replace("\n", "")
                .replace("\xa0", "")
                .strip("Long Description:")
            )
        else:
            long_description = None

        # get link social media
        if soup.find("div", {"class": "socialMedia aboutItem__value aboutItem__socialMedia"}):
            links_media = []
            social_media = soup.find("div", {"class": "socialMedia aboutItem__value aboutItem__socialMedia"})
            for link in social_media.find_all("a", href=True):
                link_media = link["href"]
                links_media.append(link_media)
        else:
            links_media = None

        # get Registered Name, Founded in and Website
        detail_overview = []
        if soup.select(".mt-24"):
            for detail in soup.select(".mt-24"):
                details = (
                    detail.text.replace("\xa0", "").replace("\n", "").replace("\t", "")
                )
                detail_overview.append(details)
        else:
            detail_overview = None

        salaries_rating = (
            soup.find("div", {"class": "rating-title"})
            .text.replace("\t", "")
            .replace("\n", "")
        )
        # get headquarter
        if soup.find("div", {"class": "textItem__val aboutItem__value"}):
            headquarter = soup.find("div", {"class": "textItem__val aboutItem__value"}).text
        else:
            headquarter = None
        # get address
        if soup.find("div", {"class": "address"}):
            address = (
                soup.find("div", {"class": "address"})
                .text.replace("\t", "")
                .replace("\n", "")
            )
        else:
            address = None

        item["overview"]["detail"] = detail_overview
        item["overview"]["description"] = long_description
        item["size"] = size
        item["address"] = address
        item["headquarter"] = headquarter
        item["salaries_raitng"] = salaries_rating
        item["links_media"] = links_media
        item["country"] = country
        item["rating"] = rating
        item["company_name"] = name
        return item

    logger.info(
        f"Crawling ambitionbox overview on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    page_exceptions = []
    error_pages = []
    try:
        response = utils.request_get(url=url, logger=logger, unblocker=True, verify=False)
        soup = BeautifulSoup(response.content)
        overview_data = extract_data_overview(soup)

        # update counts into DB for progress
        step_detail.item_count = 1
        session.commit()

        # Write to CSV to upload file
        df = pd.DataFrame([overview_data])
        src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df.to_csv(src_file, index=False, sep=DELIMITER)

        # Upload crawled file to google cloud storage
        *path, filename = src_file.split("/")
        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(src_file, dst_file)
        logger.info(f"Uploaded overview file to gcs: {dst_file}")

    except Exception as e:
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, 1, error_pages  # overview has no pages (effectively only 1)


@auto_session
def _crawl_jobs(url, step_detail, batch=None, step=None, session=None, **kwargs):
    def extract_url(url):
        pattern = r"/([a-zA-Z0-9_-]+)-jobs"
        search = re.search(pattern, url)
        company_name = search.group(1)
        url_crawl_job = f"https://www.ambitionbox.com/api/v2/search?query={company_name}&category=all&type=companies"
        response = utils.request_get(url=url_crawl_job, logger=logger, unblocker=True, verify=False, valid_status_codes=[200])
        soup = BeautifulSoup(response.content, "html.parser")
        if soup.find("pre"):
            logger.info("crawler ambitionbox job html response")
            content = soup.find("pre").find(text=True)
            data_dict = json.loads(content)
        else:
            logger.info("crawler ambitionbox job dict response")
            content = response.content
            data_dict = json.loads(content)
        company_id = data_dict["data"][0].get("companyId")
        return company_id

    def get_data_job(job_ids):
        base_job = "https://www.ambitionbox.com/api/v2/jobs/info/{job_id}"
        list_jobs = []
        for job_id in job_ids:
            job_url = base_job.format(job_id=job_id)
            response = utils.request_get(url=job_url, logger=logger, unblocker=True, verify=False, valid_status_codes=[200])
            soup = BeautifulSoup(response.content, "html.parser")
            if soup.find("pre"):
                logger.info("crawler ambitionbox get data job html response")
                content = soup.find("pre").find(text=True)
                data = json.loads(content)
            else:
                logger.info("crawler ambitionbox get data job dict response")
                content = response.content
                data = json.loads(content)
            # data = response.json()
            list_jobs.append(data["data"])

        return list_jobs

    logger.info(
        f"Crawling ambitionbox jobs on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    base_url = "https://www.ambitionbox.com/api/v2/jobs/company/{company_id}?excludeJobId=123{page}"

    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    company_id = extract_url(url)
    crawled_job_details = []
    page_exceptions = []
    total_pages = len(pages)
    error_pages = []
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
            # crawl
            page_param = f"&page={page}"
            job_page_url = base_url.format(company_id=company_id, page=page_param)
            response = utils.request_get(
                url=job_page_url, logger=logger, unblocker=True, verify=False
            )
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                if soup.find("pre"):
                    print("html response")
                    content = soup.find("pre").find(text=True)
                    page_initial_job_data = json.loads(content)
                else:
                    print("dict response")
                    content = response.content
                    page_initial_job_data = json.loads(content)
                # page_initial_job_data = response.json()
                page_job_ids = [
                    j["JobId"] for j in page_initial_job_data["data"]["Jobs"]
                ]
                crawled_job_details.extend(get_data_job(page_job_ids))

        except Exception as e:
            logger.exception(
                f"Error happens while crawling job page {page}: {job_page_url}"
            )
            page_exceptions.append(e)
            error_pages.append(page)

    # update counts into DB for progress
    step_detail.item_count = len(crawled_job_details)
    session.commit()

    # Write to CSV to upload file
    df = pd.DataFrame(crawled_job_details)
    src_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(src_file, index=False, sep=DELIMITER)

    # Upload crawled file to google cloud storage
    *path, filename = src_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(src_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(crawled_job_details)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _ambitionbox_scraper(
    url, step_detail, batch=None, step=None, session=None, **kwargs
):
    meta_data = step_detail.meta_data
    if not meta_data or meta_data.get("data_type") == "review":
        # handle crawl reviews
        return _crawl_reviews(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "review_stats":
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


def _get_crawler(strategy=STRATEGY_AMBITIONBOX):
    if strategy == STRATEGY_AMBITIONBOX:
        return _ambitionbox_scraper
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
                publish_step_detail_error(
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
                f"Unable to crawl step_detail: {step_detail_id} - url: {url}"
            )
            data = {
                "type": "crawl",
                "event": "fail",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
                "error": f"{type(e).__name__}: {str(e)}"[:200],
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
