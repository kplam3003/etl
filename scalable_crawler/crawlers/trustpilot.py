import os
import json
import datetime as dt

from google.cloud import storage
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import pandas as pd

import config
import utils
from utils import request_get
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from helpers import log_crawl_stats

logger = utils.load_logger()


DELIMITER = "\t"


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def crawl_review_stats(url, step_detail, batch=None, step=None, session=None):
    logger.info(
        f"Crawling review_stats from url: {url}, step_detail: {step_detail.step_detail_id}"
    )
    params = {"languages": "all"}
    page_exceptions = []
    error_pages = []
    total_pages = 1
    try:
        response = request_get(url=url, params=params, logger=logger, unblocker=False, verify=False)
        status_code = response.status_code
        if status_code != 200:
            logger.exception(
                f"Unable to crawl step_detail {step_detail.step_detail_id}, status_code: {status_code}, message: {response.text}"
            )
            raise Exception(
                f"Unable to crawl step_detail {step_detail.step_detail_id}, status_code: {status_code}, message: {response.text}"
            )

        # extract
        soup = BeautifulSoup(response.text, "html.parser")
        js_content = soup.select_one("script#__NEXT_DATA__").decode_contents()
        datas = json.loads(js_content)
        business_unit = (
            datas
            ["props"]
            ["pageProps"]
            ["businessUnit"]
        )
        ## Get reviews data
        review_stats_data = {}
        review_stats_data["total_reviews"] = business_unit["numberOfReviews"]
        review_stats_data["trust_score"] = business_unit["trustScore"]
        review_stats_data["average_stars"] = business_unit["stars"]

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
            item_count=int(review_stats_data["total_reviews"])
        )

    except Exception as e:
        logger.exception(f"Can't crawl review_stats from url {url}")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


def crawl_reviews(url, step_detail, batch=None, step=None, session=None):
    logger.info(
        f"Crawling reviews from url: {url}, step_detail: {step_detail.step_detail_id}"
    )

    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    pages = list(range(from_page, to_page + 1))
    total_pages = len(pages)
    page_exceptions = []
    error_pages = []
    total_reviews = []
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

        status_code = 200
        params = {"languages": "all", "page": page}

        logger.info(f"Crawling page number: {page} with url: {url}")
        try:
            response = request_get(url=url, params=params, logger=logger, unblocker=False, verify=False)
            status_code = response.status_code
            if response.status_code not in [200, 301]:
                logger.warning(
                    f"Unable to crawl step_detail {step_detail.step_detail_id}, status_code: {response.status_code}, message: {response.text}"
                )
                has_error = True
                continue

            if status_code == 301:
                # This mean no more page to crawl, the site will redirect to page 1, we should break here
                break

            logger.info(f"Extracting reviews on url: {response.url}")
            soup = BeautifulSoup(response.text, "html.parser")
            js_content = soup.select_one("script#__NEXT_DATA__").decode_contents()
            datas = json.loads(js_content)

            ## Get reviews data
            reviews = datas["props"]["pageProps"]["reviews"]
            bu_name = datas["props"]["pageProps"]["businessUnit"][
                "displayName"
            ]
            bu_id = datas["props"]["pageProps"]["businessUnit"]["id"]
            main_url = url.split("review")[0]
            for review in reviews:
                item = {}
                try:
                    item["businessUnitDisplayName"] = bu_name
                    item["consumerName"] = review["consumer"]["displayName"]
                    item["socialShareUrl"] = f"{main_url}reviews/{review['id']}"
                    item["businessUnitId"] = bu_id
                    item["reviewHeader"] = review["title"]
                    item["reviewBody"] = review["text"]
                    item["rating"] = review["rating"]
                    item["country"] = review["consumer"]["countryCode"]
                    item["comment_time"] = review["dates"]["publishedDate"]
                    item["update_time"] = review["dates"]["updatedDate"]
                    report_time = review.get("report")
                    item["reported_time"] = (
                        report_time["createdDateTime"]
                        if report_time != None
                        else report_time
                    )
                    item["link_scrape"] = url
                    logger.info(f"Trustpilot crawls item: {item}")
                    total_reviews.append(item)
                    logger.info(f"Trustpilot total items length up to now: {len(total_reviews)}")
                except:
                    logger.exception(
                        f"[ERROR] Unable to extract detailed data for review: {review}"
                    )
                    pass

        except Exception as e:
            logger.warning(
                f"Can't crawl this page number: {page} with url {url} after 5 times"
            )
            page_exceptions.append(e)
            error_pages.append(page)
            continue

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
    log_crawl_stats(
        batch=batch,
        step_detail=step_detail,
        item_count=len(total_reviews)
    )

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
        batch: ETLCompanyBatch = session.query(ETLCompanyBatch).get(
            step_detail.batch_id
        )
        step = session.query(ETLStep).get(step_detail.step_id)

        # get data_type
        meta_data = batch.meta_data
        data_type = meta_data.get("data_type") if meta_data else "review"

        try:
            if data_type == "review":
                page_exceptions, total_pages, error_pages = crawl_reviews(
                    url, step_detail, batch=batch, step=step, session=session
                )
            elif data_type == "review_stats":
                page_exceptions, total_pages, error_pages = crawl_review_stats(
                    url, step_detail, batch=batch, step=step, session=session
                )
            else:
                raise Exception(f"Not support data_type for Trustpilot: {data_type}")

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
