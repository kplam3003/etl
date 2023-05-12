import os
import config
import pandas as pd
import re
import json
import datetime as dt
import utils
from google.cloud import storage
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from bs4 import BeautifulSoup

from google.cloud import pubsub_v1
from helpers import log_crawl_stats
from utils import request_get

logger = utils.load_logger()

DELIMITER = "\t"
MOUTHSHUT_REVIEW_PAGE_SIZE = 20

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15"
}


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def _crawl_reviews(url, step_detail, batch=None, step=None, session=None):
    logger.info(f"Crawling url: {url}, step_detail: {step_detail.step_detail_id}")

    def get_review_data_in_page(soup):
        reviews = soup.find_all("div", {"class": "row review-article"})
        name = soup.find("div", {"class": "read-review-holder"})
        company_name = name.find("h2").text.replace("Reviews", "").strip()
        return reviews, company_name

    def extract_data(review_div, company_name):
        item = {}
        title = review_div.find("strong").text
        # get user_name
        user_name = review_div.find("div", {"class": "user-ms-name"}).find("a").text
        # get country
        country_div = review_div.find("div", {"class": "usr-addr-text"})
        country = country_div.text.strip() if country_div else None
        # get div rating
        rate = review_div.find("div", {"class": "rating"})
        # get time user create review
        create_at = rate.find_all("span")[1].text.replace("\n", "").strip()
        # get total view to that review
        total_comment_view = rate.find_all("span")[5].text
        # get rating star
        star = rate.find("span")
        stars = star.find_all("i", {"class": "rated-star"})
        rating = len(stars)
        # get review
        review_tag = review_div.find("div", {"class": "more reviewdata"})
        review_raw = review_tag.find_all("p")
        review_div = " ".join([r.text for r in review_raw])
        # mapping
        item["name"] = company_name
        item["title"] = title
        item["create_at"] = create_at
        item["total_comment_view"] = total_comment_view
        item["country"] = country
        item["user_name"] = user_name
        item["rating"] = rating
        item["review"] = review_div

        return item

    def handle_extract_reviews(html_content):
        extracted_review_items = []
        soup = BeautifulSoup(html_content, "lxml")
        all_review_divs, company_name = get_review_data_in_page(soup)
        for review_div in all_review_divs:
            data = extract_data(review_div, company_name)
            extracted_review_items.append(data)

        return extracted_review_items

    def extract_url(url):
        # get element necessary for crawl url
        pattern = re.compile(
            r"www.mouthshut.com/([A-Za-z-_]+)/([A-Za-z0-9-_]+)-[a-z]+-([0-9]+)"
        )
        search = re.search(pattern, url)
        cname = search.group(2)
        cid = search.group(3)
        url_crawl = (
            f"https://www.mouthshut.com/Review/rar_reviews.aspx?cname={cname}&cid={cid}"
        )
        return url_crawl

    total_reviews = []
    page_exceptions = []
    error_pages = []

    url_crawl = extract_url(url)
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
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
        future.result()
        logger.info(f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%")

        # crawl url
        try:
            logger.info(f"Crawling page number: {page} with url: {url}")
            url_rv = url_crawl + "&page=" + str(page)
            response = request_get(
                url=url_rv,
                unblocker=False,
                logger=logger,
                headers=headers,
                valid_status_codes=[200],
            )

            html_content = response.text
            page_reviews = handle_extract_reviews(html_content)
            total_reviews.extend(page_reviews)

        except Exception as e:
            logger.exception(f"MOUTHSHUT: Cannot crawl url {url_rv}")
            page_exceptions.append(e)
            error_pages.append(page)

    # save crawled item count
    step_detail.item_count = len(total_reviews)
    session.commit()

    # write CSV file locally
    df = pd.DataFrame(total_reviews)
    df.drop_duplicates(inplace=True)
    mkdirs_if_not_exists(config.OUTPUT_DIR)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    df.to_csv(output_file, index=False, sep=DELIMITER)

    # upload csv to GCS
    *path, filename = output_file.split("/")
    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(batch=batch, step_detail=step_detail, item_count=len(total_reviews))

    return page_exceptions, total_pages, error_pages


def _crawl_review_stats(url, step_detail, batch=None, step=None, session=None):
    logger.info(f"Crawling url: {url}, step_detail: {step_detail.step_detail_id}")

    total_pages = 1
    page_exceptions = []
    error_pages = []
    try:
        response = request_get(
            url=url,
            unblocker=False,
            logger=logger,
            headers=headers,
            valid_status_codes=[200],
        )
        soup = BeautifulSoup(response.content, "html.parser")
        json_script = " ".join(
            soup.find("script", {"type": "application/ld+json"}).string.split()
        )
        logger.info(f"Extracted JSON script: {json_script}")
        data = json.loads(json_script)
        review_stats_data = {}
        review_stats_data["total_ratings"] = float(
            data["aggregateRating"]["ratingCount"]
        )
        review_stats_data["average_rating"] = float(
            data["aggregateRating"]["ratingValue"]
        )

        # save crawled item count
        step_detail.item_count = 1
        session.commit()

        # write CSV file locally
        df = pd.DataFrame([review_stats_data])
        df.drop_duplicates(inplace=True)
        mkdirs_if_not_exists(config.OUTPUT_DIR)
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
            item_count=int(review_stats_data["total_ratings"]),
        )

    except Exception as e:
        logger.exception(f"MOUTHSHUT: Cannot crawl review_stats from url {url}")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        # Crawl data from source website
        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        # get data_type
        meta_data = batch.meta_data
        data_type = meta_data.get("data_type")

        try:
            if data_type == "review":
                page_exceptions, total_pages, error_pages = _crawl_reviews(
                    url, step_detail, batch=batch, step=step, session=session
                )
            elif data_type == "review_stats":
                page_exceptions, total_pages, error_pages = _crawl_review_stats(
                    url, step_detail, batch=batch, step=step, session=session
                )
            else:
                raise Exception(f"Unsupported data_type for Mouthshut: {data_type}")

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
