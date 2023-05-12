import config
import pandas as pd
from google.cloud import storage
import json
import os
import datetime as dt
import utils
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
import praw
from urllib.parse import urlsplit, parse_qs
import requests


from google.cloud import pubsub_v1
from helpers import log_crawl_stats

logger = utils.load_logger()

DELIMITER = "\t"

HEADERS = {
    "authority": "www.capterra.com",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36",
}

MAX_REVIEW_LENGTH = 2000


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def __process_crawled_review(review_item):
    for key in ["selftext", "body"]:
        review_text = review_item.get(key)
        if not review_text:
            continue

        try:
            review_text = str(review_text)
        except:
            review_text = ""

        processed_review_text = review_text.replace("\t", " ").replace("\n", " ")
        if len(processed_review_text) > MAX_REVIEW_LENGTH:
            processed_review_text = processed_review_text[:MAX_REVIEW_LENGTH]

        review_item[key] = processed_review_text

    return review_item


def _prepare_query_dict(url):
    ret_query_dict = dict()

    url_split = urlsplit(url)
    query_dict = parse_qs(url_split.query)
    subreddit_names = query_dict.get("sub_reddits", [])
    subreddit_str = ""
    if len(subreddit_names):
        subreddit_names_list = subreddit_names[0].split(",")
        ret_query_dict["subreddits"] = subreddit_names_list
        subreddit_str = "+".join(subreddit_names_list)

    ret_query_dict["subreddits_query"] = subreddit_str

    keywords = query_dict.get("keywords", [])
    keywords_str = ""
    if len(keywords):
        keywords_list = keywords[0].split(",")
        ret_query_dict["keywords"] = keywords_list

        mapped_keywords_list = []
        for keyword in keywords_list:
            mapped_keywords_list.extend([f"selftext:{keyword}", f"title:{keyword}"])
        keywords_str = " OR ".join(mapped_keywords_list)

    ret_query_dict["keywords_query"] = keywords_str

    return ret_query_dict


@utils.retry(5)
def request_get(url, limit=200, *args, **kwargs):
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
    request_session = requests.Session()
    request_session.verify = False
    # NOTE: Here we should not use Luminati proxy because we are identifying as a user
    # So changing IPs may get us blocked
    # Also, reddit is not that frequently used, so no need for unblocking.

    reddit = praw.Reddit(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_CLIENT_SECRET,
        user_agent=user_agent,
        requestor_kwargs={"session": request_session},
    )

    query_dict = _prepare_query_dict(url)

    subreddit = reddit.subreddit(query_dict["subreddits_query"])

    search_params = {"sort": "new", "limit": limit}
    params = {}
    after = kwargs.get("after", None)
    params = {"after": after}

    submissions = subreddit.search(
        query_dict["keywords_query"], **search_params, params=params
    )

    results = []
    for submission in submissions:
        # Parse data into dict
        submission_dict = {
            key: str(value)
            for key, value in submission.__dict__.items()
            if key != "_reddit"
        }
        results.append(__process_crawled_review(submission_dict))

        submission.comment_sort = "new"
        submission.comments.replace_more(limit=None)
        comment_queue = submission.comments[:]  # Seed with top-level
        while comment_queue:
            comment = comment_queue.pop(0)
            comment_dict = {
                key: str(value)
                for key, value in comment.__dict__.items()
                if key != "_reddit"
            }

            comment_body = comment_dict.get("body", "")
            if not query_dict["keywords"] or (
                comment_body
                and any(word in comment_body for word in query_dict["keywords"])
            ):
                results.append(__process_crawled_review(comment_dict))

            comment_queue.extend(comment.replies)

        after = submission_dict["name"]

    return results, after


def _crawl_submissions(
    url, step_detail, after=None, batch=None, step=None, session=None
):

    logger.info(f"Crawling url: {url}, step_detail: {step_detail.step_detail_id}")

    total_comments = []

    # format 1:10
    from_page, to_page = step_detail.paging.split(":")
    pages = list(range(int(from_page), int(to_page) + 1))
    total_pages = len(pages)
    page_exceptions = []
    error_pages = []
    for index, _ in enumerate(pages):
        # Publish progress using PubSub
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
        result = future.result()
        logger.info(f"Published crawl progress {((index + 1) / len(pages)) * 100:.2f}%")

        params = {"after": after} if after else {}

        results = []
        try:
            results, after = request_get(url, limit=config.REDDIT_PAGE_SIZE, verify=False, **params)
        except Exception as e:
            logger.exception(f"REDDIT: Cannot crawl url {url}, page {index + 1}")
            page_exceptions.append(e)
            error_pages.append(index + 1)

        total_comments.extend(results)
        logger.info(f"Crawled {len(total_comments)} review items.")

    # save crawled item count
    step_detail.item_count = len(total_comments)
    session.commit()

    # write CSV file locally
    df = pd.DataFrame(total_comments)
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
        batch=batch, step_detail=step_detail, item_count=len(total_comments)
    )

    return page_exceptions, total_pages, error_pages, after


def _crawl_submission_stats(url, step_detail, batch=None, step=None, session=None):

    logger.info(f"Crawling url: {url}, step_detail: {step_detail.step_detail_id}")
    total_pages = 1
    page_exceptions = []
    error_pages = []
    try:
        meta_data = step_detail.meta_data
        total_submissions = meta_data.get("total_submissions")
        submission_stats_data = {"total_submissions": total_submissions}

        # write CSV file locally
        df = pd.DataFrame([submission_stats_data])
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
            item_count=int(submission_stats_data["total_submissions"]),
        )

    except Exception as e:
        logger.exception(f"REDDIT: Unable to get total_submissions")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


@auto_session
def execute_task(task, session=None):
    after_token = None

    url = task.get("url")
    ids = task.get("ids")

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        meta_data = batch.meta_data
        data_type = meta_data.get("data_type")

        # Crawl data from source website
        try:
            if data_type == "review":
                (
                    page_exceptions,
                    total_pages,
                    error_pages,
                    after_token,
                ) = _crawl_submissions(
                    url,
                    step_detail,
                    after_token,
                    batch=batch,
                    step=step,
                    session=session,
                )
            elif data_type == "review_stats":
                page_exceptions, total_pages, error_pages = _crawl_submission_stats(
                    url, step_detail, batch=batch, step=step, session=session
                )
            else:
                raise Exception(f"Unsupported data_type for Reddit: {data_type}")

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
                f"Unable to crawl step detail {step_detail.step_detail_id} due to error",
                e,
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
