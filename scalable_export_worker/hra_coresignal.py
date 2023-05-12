import os
import sys

sys.path.append("../")

from google.cloud import bigquery

import config
from core import pubsub_util, logger
from core.operation import auto_killer
import bigquery_helpers
import utils

from hra_coresignal_tenure import insert_coresignal_employees_tenure
from hra_coresignal_turnover import insert_company_employees_bucket
from hra_coresignal_summary_table import (
    delete_summary_table_hra,
    create_hra_summary_table_company,
    create_hra_summary_table_education,
    create_hra_summary_table_education_degree,
    create_hra_summary_table_employees,
    create_hra_summary_table_experience,
    create_hra_summary_table_monthly_dataset,
    create_hra_summary_table_turnover,
    insert_hra_summary_table_company,
    insert_hra_summary_table_education,
    insert_hra_summary_table_education_degree,
    insert_hra_summary_table_employees,
    insert_hra_summary_table_experience,
    insert_hra_summary_table_monthly_dataset,
    insert_hra_summary_table_turnover,
)

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

EXPORTED_CHARTS_METADATA = (
    # csv exports for VoC charts
    "HRA_1",
    "company_profiles",
    "employee_profiles",
    "employee_experiences",
    "monthly_dataset",
    "education_dataset",
    "turnover_dataset"
)

ALL_SUMMARY_TABLE_PREFIXES = (
    config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_COMPANY_PREFIX,
    config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX,
    config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EXPERIENCE_PREFIX,
    config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_MONTHLY_DATASET_PREFIX,
    config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_PREFIX,
)


def export_all_hra_chart(bqclient, payload, run_id):
    meta_data = {}
    for export_item in payload["payload"]["exports"]:
        chart_code = export_item["code"]

        # only export chart codes that are in EXPORTED_CHARTS_METADATA
        if chart_code in EXPORTED_CHARTS_METADATA:
            chart_metadata_from_payload = {
                "datamart_table": export_item["table"],
                "prefix_filename": export_item["prefix"],
            }
            meta_data[chart_code] = chart_metadata_from_payload
            bigquery_helpers.export_chart(
                bqclient, chart_code, payload, meta_data, logger
            )


@auto_killer
def handle_task_hra(payload, set_terminator=None):
    try:
        case_study_id = payload["case_study_id"]
        run_id = payload["postprocess"]["meta_data"]["run_id"]
        request_payload = payload["payload"]

        bqclient = bigquery.Client()
        # update initial progress
        utils.update_progress(
            payload=payload,
            worker_progress=0.0,
            project_id=config.GCP_PROJECT_ID,
            progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
            logger=logger,
        )

        # Set terminator for before kill execution
        def _terminator():
            logger.info("Graceful terminating...")
            pubsub_util.publish(
                logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC, payload
            )
            logger.info(
                f"Republishing a message to {config.GCP_PUBSUB_TOPIC}: {payload}"
            )
            os._exit(1)

        set_terminator(_terminator)

        # insert run_id
        bigquery_helpers.insert_case_study_run_id(
            logger=logger,
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            run_id=run_id,
            table_name=config.GCP_BQ_TABLE_HRA_CASESTUDY_RUN_ID,
        )

        # TODO: insert modified dimensions
        # => not needed yet. Will need implementation for modified dimensions:
        # 1. add modified_job_function and modified_job_function_group to hra_casestudy_dimension_config
        # 2. modify insert function to also insert these info

        # insert alias list
        # this is for searching/filtering using company aliases
        bigquery_helpers.insert_company_aliases(
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            companies_data_payload=request_payload["company_datasources"],
            run_id=run_id,
            table_company_aliases_list=config.GCP_BQ_TABLE_HRA_COMPANY_ALIASES_LIST,
            logger=logger,
        )

        # create summary tables
        ## first truncate/delete existing summary table
        logger.info("Deleting standing summary tables, if any...")
        for prefix in ALL_SUMMARY_TABLE_PREFIXES:
            delete_summary_table_hra(
                bqclient=bqclient,
                case_study_id=case_study_id,
                table_summary_table_prefix=prefix,
            )

        ## recreate and insert summary tables
        ### company
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_COMPANY_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_company(bqclient, case_study_id)
        insert_hra_summary_table_company(
            bqclient=bqclient, case_study_id=case_study_id, run_id=run_id
        )

        ### employees
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_employees(bqclient, case_study_id)
        insert_hra_summary_table_employees(
            bqclient=bqclient, case_study_id=case_study_id, run_id=run_id
        )

        ### employees tenure
        logger.info(
            f"Calculating tenure fields for experience dataset, and the monthly dataset: {case_study_id}"
        )
        insert_coresignal_employees_tenure(
            case_study_id=case_study_id, case_payload=request_payload
        )

        ### experiences
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EXPERIENCE_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_experience(bqclient, case_study_id)
        insert_hra_summary_table_experience(
            bqclient=bqclient, case_study_id=case_study_id, run_id=run_id
        )
        
        ### monthly dataset
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_MONTHLY_DATASET_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_monthly_dataset(bqclient, case_study_id)
        insert_hra_summary_table_monthly_dataset(
            bqclient=bqclient, case_study_id=case_study_id, run_id=run_id
        )

        ### education
        # NOTE: Stop creating un-use table
        # logger.info(
        #     f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_PREFIX}_{case_study_id}..."
        # )
        # create_hra_summary_table_education(bqclient, case_study_id)
        # insert_hra_summary_table_education(
        #     bqclient=bqclient, case_study_id=case_study_id, run_id=run_id
        # )

        ### education degree
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_DEGREE_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_education_degree(bqclient, case_study_id)
        insert_hra_summary_table_education_degree(
            bqclient=bqclient,
            case_study_id=case_study_id,
            run_id=run_id,
            edu_dim_conf=request_payload["edu_dimension_config"],
        )

        ### employee bucket
        logger.info(
            f"Aggregating employee bucket: {case_study_id}"
        )
        insert_company_employees_bucket(case_study_id=case_study_id, case_payload=request_payload)

        ### employee turnover
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_turnover(bqclient=bqclient, case_study_id=case_study_id)
        insert_hra_summary_table_turnover(
            bqclient=bqclient,
            case_study_id=case_study_id,
            case_study_name=request_payload["case_study_name"],
            run_id=run_id,
        )

        ### employee bucket
        logger.info(
            f"Aggregating employee bucket: {case_study_id}"
        )
        insert_company_employees_bucket(case_study_id=case_study_id, case_payload=request_payload)

        ### employee turnover
        logger.info(
            f"Recreate and insert {config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}..."
        )
        create_hra_summary_table_turnover(bqclient=bqclient, case_study_id=case_study_id)
        insert_hra_summary_table_turnover(
            bqclient=bqclient,
            case_study_id=case_study_id,
            case_study_name=request_payload["case_study_name"],
            run_id=run_id,
        )

        # TODO: create exported CSV
        logger.info(f"Begin export_all_chart VoE")
        export_all_hra_chart(bqclient, payload, run_id)

        # done, publish finish event
        pubsub_util.publish(
            logger,
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
            {
                "event": "finish",
                "type": "export",
                "run_id": run_id,
                "postprocess": payload["postprocess"],
                "case_study_id": payload["case_study_id"],
                "request_id": payload["request_id"],
                "error": "",
            },
        )
        set_terminator(None)

    except Exception as error:
        pubsub_util.publish(
            logger,
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
            {
                "event": "fail",
                "type": "export",
                "run_id": run_id,
                "postprocess": payload["postprocess"],
                "case_study_id": payload["case_study_id"],
                "request_id": payload["request_id"],
                "error": str(error),
            },
        )
        logger.exception(f"Error: {error}")
