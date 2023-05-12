from database import (
    Session,
    ETLCompanyBatch,
    ETLStep,
    ETLStepDetail,
    ETLDatasource,
    auto_session,
)
import utils
import requests
import config
import pandas as pd
import datetime as dt
import json
import math
from google.cloud import pubsub_v1


STRATEGY_CSV = "csv"
STRATEGY_EXCEL = "excel"
STRATEGY_PARQUET = "parquet"
STRATEGY_HDFS = "hdfs"

logger = utils.load_logger()


@auto_session
def load_etl_step(batch: ETLCompanyBatch, session=None):
    # If not yet etl step for crawl is generated, then create one
    etl_step = (
        session.query(ETLStep)
        .filter(ETLStep.batch_id == batch.batch_id)
        .filter(ETLStep.step_type == "crawl")
        .first()
    )
    if not etl_step:
        etl_step = ETLStep()
        etl_step.step_name = (
            f"crawl-{batch.company_name.lower()}-{batch.source_code.lower()}"
        )
        etl_step.status = "running"
        etl_step.created_at = dt.datetime.today()
        etl_step.request_id = batch.request_id
        etl_step.batch_id = batch.batch_id
        etl_step.step_type = "crawl"
        etl_step.updated_at = dt.datetime.today()
        session.add(etl_step)
        session.commit()
    else:
        etl_step.status = "running"
        session.commit()

    return etl_step


@auto_session
def dispatch_tasks(
    total,
    batch: ETLCompanyBatch,
    strategy=STRATEGY_CSV,
    session=None,
    data_type="review",
):
    today = dt.datetime.today()
    url = batch.url

    logger.info(f"Total reviews found for url {url}: {total}")
    meta_data = batch.meta_data or {}
    paging_info = meta_data.get("paging_info", {})
    by_langs = paging_info.get("by_langs", {})
    by_langs["en"] = total
    paging_info["by_langs"] = by_langs
    paging_info["total"] = sum([by_langs[k] for k in by_langs.keys()])
    meta_data["paging_info"] = paging_info
    batch.meta_data = meta_data
    session.commit()

    pages = math.ceil(total / config.ROWS_PER_STEP) or 1
    steps = math.ceil(total / config.ROWS_PER_STEP) or 1
    paging = math.ceil(config.ROWS_PER_STEP / config.ROWS_PER_STEP)

    etl_step = load_etl_step(batch, session=session)

    logger.info(f"Generating step details, there are {steps} steps")
    publisher = pubsub_v1.PublisherClient()
    project_id = config.GCP_PROJECT
    topic_id = config.GCP_PUBSUB_TOPIC_CRAWL
    topic_path = publisher.topic_path(project_id, topic_id)

    ids = []
    if data_type == "review_stats":
        start = 1
        end = 1
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = f"{data_type}-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}".strip()
        step_detail.status = "waiting"
        step_detail.step_id = etl_step.step_id
        step_detail.batch_id = batch.batch_id
        step_detail.file_id = 1
        step_detail.paging = f"{start}:{end}"
        step_detail.created_at = today
        step_detail.updated_at = today
        step_detail.progress_total = 1
        step_detail.progress_current = 0
        step_detail.lang = "en"

        meta_data = batch.meta_data or {}
        meta_data["url"] = url
        meta_data["data_type"] = data_type
        step_detail.meta_data = meta_data

        session.add(step_detail)
        session.commit()
        ids.append(step_detail.step_detail_id)

    else:
        for step in range(steps):
            start = step * paging + 1
            end = (list(range(pages + 1)[start : start + paging]) or [1])[-1]
            step_detail = ETLStepDetail()
            step_detail.request_id = batch.request_id
            step_detail.step_detail_name = f"{data_type}-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{step + 1:03d}".strip()
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

            meta_data = batch.meta_data or {}
            meta_data["url"] = url
            meta_data["data_type"] = data_type
            step_detail.meta_data = meta_data

            session.add(step_detail)
            session.commit()
            ids.append(step_detail.step_detail_id)

    payload = json.dumps(
        {
            "url": url,
            "batch_id": batch.batch_id,
            "strategy": strategy,
            "ids": ids,
            "data_type": data_type,
        }
    )
    payload = payload.encode("utf-8")
    logger.info(f"Publishing a message to {topic_id}: {payload}")
    future = publisher.publish(topic_path, payload)
    logger.info(f"Published message to {topic_id}, result: {future.result()}")
    logger.info(f"Assign task completed")


@auto_session
def csv_scrape(batch: ETLCompanyBatch, session=None, **kwargs):
    url = batch.url
    chunks = pd.read_csv(url, chunksize=config.ROWS_PER_STEP)
    total = 0
    for chunk in chunks:
        total += chunk.shape[0]

    meta_data = batch.meta_data
    data_type = meta_data.get("data_type") if meta_data else "review"
    dispatch_tasks(
        total, batch, strategy=STRATEGY_CSV, session=session, data_type=data_type
    )


def get_scraper(strategy=STRATEGY_CSV):
    if strategy == STRATEGY_CSV:
        return csv_scrape
    elif strategy == STRATEGY_EXCEL:
        raise NotImplemented("Strategy EXCEL not supported yet")
    elif strategy == STRATEGY_PARQUET:
        raise NotImplemented("Strategy PARQUET not supported yet")
    elif strategy == STRATEGY_HDFS:
        raise NotImplemented("Strategy HDFS not supported yet")
    else:
        return csv_scrape
