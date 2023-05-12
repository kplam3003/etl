import sys

sys.path.append("../")

import datetime
import json

from google.cloud import bigquery

from core import pubsub_util
from core.logger import init_logger
import config

# initialization
logger = init_logger(config.LOGGER_NAME, config.LOGGER)
project_id = config.GCP_PROJECT_ID
subscription_id = config.GCP_PUBSUB_SUBSCRIPTION
etl_error_table_id = config.GCP_BQ_TABLE_ETL_ERRORS


def handle_task_company_datasource(payload):
    """
    Collect error messages from internal error topics
    and write to bigquery table for error tracking
    """
    bq_client = bigquery.Client()
    etl_error_table = bq_client.get_table(etl_error_table_id)
    schema = etl_error_table.schema
    field_names = [col.name for col in schema]

    # create a record
    record = {key: val for key, val in payload.items() if key in field_names}
    record["created_time"] = int(datetime.datetime.now().timestamp())
    # additional_info is a json string
    if record["additional_info"]:
        record["additional_info"] = json.dumps(record["additional_info"])
    else:
        record["additional_info"] = "{}"  # empty json

    # write record to BQ
    errors = bq_client.insert_rows_json(table=etl_error_table_id, json_rows=[record])
    if not errors:
        logger.info(f"New record added to {etl_error_table_id}: {record}")
    else:
        logger.error("Encountered errors while inserting rows: {}".format(errors))

if __name__ == "__main__":
    pubsub_util.subscribe(
        logger=logger,
        gcp_product_id=project_id,
        gcp_pubsub_subscription=subscription_id,
        handle_task=handle_task_company_datasource,
    )
