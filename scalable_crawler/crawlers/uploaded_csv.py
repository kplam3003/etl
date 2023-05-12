import config
import logging
import pandas as pd
from google.cloud import storage
import re
import json
import os
import datetime as dt
import utils
from database import Session, ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session

from google.cloud import pubsub_v1

logger = utils.load_logger()

DELIMITER = "\t"


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def handle_review_stats(url, ids, session=None):
    chunks = pd.read_csv(url, chunksize=config.ROWS_PER_STEP)
    step_detail_id = ids[0]
    total_reviews = 0
    total_ratings = 0
    for chunk in chunks:
        total_reviews += chunk.shape[0]
        total_ratings += chunk["rating"].astype("float").sum()

    review_stats_data = {}
    review_stats_data["total_ratings"] = [total_ratings]
    review_stats_data["total_reviews"] = [total_reviews]
    review_stats_data["average_rating"] = [
        total_ratings / total_reviews if total_reviews else None
    ]
    review_stats_df = pd.DataFrame(review_stats_data)

    # Write crawling status to task db
    step_detail = session.query(ETLStepDetail).get(step_detail_id)
    step_detail.status = "running"
    step_detail.updated_at = dt.datetime.today()
    session.commit()

    batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
    step = session.query(ETLStep).get(step_detail.step_id)

    # Crawl data from source website
    try:
        mkdirs_if_not_exists(config.OUTPUT_DIR)
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        review_stats_df.to_csv(output_file, index=False, sep=DELIMITER)

        step_detail.item_count = 1
        session.commit()

        # Upload crawled file to google cloud storage
        *path, filename = output_file.split("/")

        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        logger.info(f"Uploaded file to gcs: {dst_file}")

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
        future = publisher.publish(topic_path, data)
        result = future.result()
        logger.info(f"Message published to Pub/Sub {result}")

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
        future = publisher.publish(topic_path, data)
        result = future.result()
        logger.info(f"Message published to Pub/Sub {result}")


def handle_csv(url, ids, session=None):
    chunks = pd.read_csv(url, chunksize=config.ROWS_PER_STEP)

    for step_detail_id, chunk in zip(ids, chunks):
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        # Crawl data from source website
        try:
            mkdirs_if_not_exists(config.OUTPUT_DIR)
            output_file = (
                f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
            )
            chunk.to_csv(output_file, index=False, sep=DELIMITER)

            step_detail.item_count = chunk.shape[0]
            session.commit()

            # Upload crawled file to google cloud storage
            *path, filename = output_file.split("/")

            dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
            upload_google_storage(output_file, dst_file)
            logger.info(f"Uploaded file to gcs: {dst_file}")

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
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")

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
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    data_type = task.get("data_type", "review")
    if data_type == "review_stats":
        handle_review_stats(url, ids, session)
    else:
        handle_csv(url, ids, session)


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
