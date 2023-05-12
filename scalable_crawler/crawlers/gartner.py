import config
import pandas as pd
import re
import json
import os
import datetime as dt
import utils
import requests
import itertools

from bs4 import BeautifulSoup
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from retry import retry

from google.cloud import storage
from google.cloud import pubsub_v1
from helpers import log_crawl_stats


logger = utils.load_logger()


STRATEGY_GARTNER = "gartner"
STRATEGY_LUMINATI = "luminati"
STRATEGY_WEBWRAPPER = "webwrapper"
STRATEGY_DATASHAKE = "datashake"

GARTNER_URL = "https://www.gartner.com/reviews/api2-proxy/reviews/market/vendor/filter"
GARTNER_REVIEW_DETAIL_BASE_URL = "https://www.gartner.com/reviews/review/view/{}"
DATA_COLLECTOR_BASE_URL = "https://api.luminati.io/dca/crawl"
DELIMITER = "\t"


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


@retry(tries=10, delay=1, backoff=2)
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
        logger.warning(f"Can't get Gartner html from this url {res.url}")
        logger.warning(f"Retrying....")
        logger.exception(f"Error {res.status_code}, response:{res.text[:100]}")
        raise Exception(f"Error {res.status_code}, response:{res.text[:100]}")
    return res


def get_review_technical_details(review_id, url):
    """
    Get review content with greater detail from Gartner
    using given review id
    """
    technical_detail_url = "/".join(url.split("/")[:10] + ["review", "view", str(review_id)])
    # get and parse data from HTML
    try:
        if review_id is None:
            raise Exception("reviewId is None")

        response = utils.request_get(
            technical_detail_url,
            logger=logger,
            unblocker=True,
            verify=False
        )
        soup = BeautifulSoup(response.content, "lxml")
        __NEXT_DATA__script = soup.find("script", {"id": "__NEXT_DATA__"})
        # this script may not be available
        if __NEXT_DATA__script:
            raw_technical_review = json.loads(
                soup.find("script", {"id": "__NEXT_DATA__"}).decode_contents()
            )
        else:
            raise Exception(
                f"__NEXT_DATA__ script not found from: {technical_detail_url}."
            )

        raw_representation = (
            raw_technical_review.get("props", {})
            .get("pageProps", {})
            .get("serverSideXHRData", {})
            .get("getReviewPresentation", {})
            .get("review", {})
        )
        qa_sections = raw_representation.get("sections", [])
        nested_qas = [d.get("questions", []) for d in qa_sections]
        qa_results = {qa["key"]: qa["value"] for qa in itertools.chain(*nested_qas)}

    except Exception:
        logger.exception("Error happens while parsing Gartner technical detail")
        raw_representation = {}
        qa_results = {}

    finally:
        return raw_representation, qa_results


def augment_review_summary(review_data: dict, url: str) -> dict:
    """
    Augment review data with its technical details
    """
    review_id = review_data.get("reviewId")
    raw_repr, qa_results = get_review_technical_details(review_id, url)
    new_review_data = review_data.copy()
    new_review_data.update(raw_repr)
    new_review_data.update(qa_results)
    return new_review_data


@auto_session
def _crawl_review_stats(
    url, step_detail, batch=None, step=None, session=None, **kwargs
):
    def extract_rating(soup):
        ratings_data = {}
        raw_technical_review = json.loads(
            soup.find("script", {"id": "__NEXT_DATA__"}).decode_contents()
        )
        raw_representation = (
            raw_technical_review.get("props").get("pageProps").get("serverSideXHRData")
        )
        # get average rating
        # NOTE: this info is required so we let it fail
        ratings_data["average_rating"] = (
            raw_representation.get("source-ratings-product")
            .get("productView")
            .get("vendorReviewDTO")
            .get("averageRating")
        )
        # get detail rating
        # NOTE: not required, so allowed to be null
        ratings = (
            raw_representation.get("source-ratings-product", {})
            .get("productView", {})
            .get("overAllRatingStar", {})
        )
        index = list(ratings.keys())
        for i in range(len(ratings)):
            ratings_data[index[i] + "_star"] = ratings[index[i]]
        # get category ratings
        # NOTE: not required, so allowed to be null
        category_ratings = raw_representation.get("product-ratings", {}).get(
            "sectionRatings", []
        )
        for i in range(len(category_ratings)):
            ratings_data[category_ratings[i][0].get("title")] = category_ratings[i][
                0
            ].get("reviewRating")
        # get total ratings
        # NOTE: also required
        ratings_data["total_ratings"] = (
            raw_representation.get("source-ratings-product")
            .get("productView")
            .get("vendorReviewDTO")
            .get("ratingsCount")
        )
        # get total reviews
        # NOTE: also required
        ratings_data["total_reviews"] = raw_representation.get(
            "source-ratings-product"
        ).get("totalReviewCount")
        return ratings_data

    def handle_extract_review_stats(page_html_content):
        page_soup = BeautifulSoup(page_html_content, "lxml")
        return extract_rating(page_soup)

    logger.info(
        f"Crawling gartner review stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )
    page_exceptions = []
    total_pages = 1
    error_pages = []

    # Crawl
    review_stats = {}
    try:
        response = request_get(url)
        if response.status_code == 200:
            page_html_content = response.text
            review_stats = handle_extract_review_stats(page_html_content)

    except Exception as e:
        logger.exception("Will skip this page.")
        page_exceptions.append(e)
        error_pages.append(0)

    # save crawled item count
    step_detail.item_count = 1 if review_stats else 0
    session.commit()

    # early stopping: if can't crawl review_stats
    if not review_stats:
        return page_exceptions, total_pages, error_pages

    # write CSV file locally
    df = pd.DataFrame([review_stats])
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
        item_count=int(review_stats["total_reviews"])
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _crawl_reviews(url, step_detail, batch=None, step=None, session=None, **kwargs):
    logger.info(
        f"Crawling gartner reviews on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )
    total_reviews = []
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
    page_exceptions = []
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
        # Crawl
        try:
            market = re.search(r"/market/([^/]+)", url).group(1)
            vendor = re.search(r"/vendor/([^/]+)", url).group(1)
            product = re.search(r"/product/([^/]+)", url).group(1)
            end_index = page * config.GARTNER_REVIEW_PAGE_SIZE
            start_index = end_index - config.GARTNER_REVIEW_PAGE_SIZE + 1
            params = (
                ("vendorSeoName", vendor),
                ("marketSeoName", market),
                ("productSeoName", product),
                ("startIndex", start_index),
                ("endIndex", end_index)
            )
            response = request_get(GARTNER_URL, params=params, verify=False)
            body = json.loads(response.text)
            user_reviews = body.get("userReviews", [])
            augmented_user_reviews = list(map(augment_review_summary, user_reviews, [url]*len(user_reviews)))
            total_reviews = [*total_reviews, *augmented_user_reviews]
            logger.info(f"Crawled {len(total_reviews)} review items")

        except Exception as e:
            logger.exception(
                f"Error happens while crawling page {page}, "
                f"start_index {start_index}, end_index {end_index}. "
                "Will skip this page."
            )
            page_exceptions.append(e)
            error_pages.append(page)

    # save crawled item count
    step_detail.item_count = len(total_reviews)
    session.commit()

    # write CSV file locally
    df = pd.DataFrame(total_reviews)
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
        item_count=len(total_reviews)
    )

    return page_exceptions, total_pages, error_pages


@auto_session
def _gartner_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
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
    else:
        raise Exception(f"Unsupported strategy: {meta_data}")


def _get_crawler(strategy=STRATEGY_GARTNER):
    logger.info(f"Use crawler strategy {strategy}")
    if strategy == STRATEGY_GARTNER:
        return _gartner_scraper
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
                "error": f"Something went wrong: {str(e)}",
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
