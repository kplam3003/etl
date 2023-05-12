from google.cloud import storage
from google.cloud import bigquery

import utils
import config
from core import etl_const
from database import ETLStepDetail, ETLCompanyBatch

logger = utils.load_logger()


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)


def log_crawl_stats(
    batch: ETLCompanyBatch, step_detail: ETLStepDetail, item_count: int
):
    bq_client = bigquery.Client()
    nlp_type = str(batch.nlp_type).lower()
    data_type = batch.meta_data["data_type"]
    logger.info(f"log crawl stats nlp_type : {nlp_type} and data_type {data_type}")

    bq_statistic_table = ""
    if nlp_type == etl_const.Meta_NLPType.VOE.value.lower():
        bq_statistic_table = config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS
    elif nlp_type == etl_const.Meta_NLPType.VOC.value.lower():
        bq_statistic_table = config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS
    elif nlp_type == etl_const.Meta_NLPType.HR.value.lower():
        bq_statistic_table = config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS

    row = {
        "request_id": batch.request_id,
        "company_datasource_id": batch.company_datasource_id,
        "step_detail_id": step_detail.step_detail_id,
        "created_at": step_detail.created_at.isoformat(),
        "company_id": batch.company_id,
        "company_name": batch.company_name,
        "source_id": batch.source_id,
        "source_name": batch.source_name,
        "batch_id": batch.batch_id,
        "num_reviews": item_count,
        "data_version": batch.data_version,
        "data_type": f"origin_{data_type}",
    }
    logger.info(f"Inserting row {row} into Big query table {bq_statistic_table}")
    bq_errors = bq_client.insert_rows_json(bq_statistic_table, [row])
    return bq_errors
