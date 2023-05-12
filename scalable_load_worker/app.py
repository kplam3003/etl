import sys
import csv

sys.path.append("../")
# extend limit of CSV file
csv.field_size_limit(sys.maxsize)

import config
from core import pubsub_util, logger, etl_util_v2, common_util, etl_const
import json
from datetime import datetime
import re
from dateparser import parse
from google.cloud import bigquery
from pymongo import MongoClient, errors as pm_errors

from coresignal import (
    transform_func_coresignal_stats,
    transform_func_coresignal_employees,
    _load_coresignal_linkedin_employee,
)


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
DELIMITER = "\t"

etlHandler = None

client = None
db = None


def transform_item(payload, item):
    nlp_type = payload["batch"]["nlp_type"]
    result = []

    item_tmp = {}
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["review_id"] = item["review_id"]
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["request_id"] = payload["batch"]["request_id"]
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["nlp_type"] = nlp_type
    item_tmp["user_name"] = item["user_name"]
    item_tmp["review_date"] = parse(item["__review_date__"]).isoformat()
    item_tmp["review_language"] = item["language"]
    item_tmp["review"] = item["review"]
    item_tmp["trans_review"] = item["trans_review"]
    item_tmp["trans_status"] = item["trans_status"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]

    if item.get("review_country"):
        item_tmp["review_country"] = item.get("review_country")
    else:
        item_tmp["review_country"] = "Unknown"

    if item.get("review_country_code"):
        item_tmp["review_country_code"] = item.get("review_country_code")
    else:
        item_tmp["review_country_code"] = "Unknown"

    # for parent review and technical type mapping
    item_tmp["parent_review_id"] = item["parent_review_id"]
    item_tmp["technical_type"] = item["technical_type"]

    # review hash, generated from preprocessing step, for mark review is existed
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["review_hash"] = item["review_hash"]

    item_tmp["rating"] = item["rating"]
    try:
        item_tmp["rating"] = float(item_tmp["rating"])
    except:
        logger.info(f"Can not case {item_tmp['rating']} into float -> fallback to None")
        item_tmp["rating"] = None

    item_tmp["data_version"] = payload["batch"]["data_version"]

    result.append(item_tmp)

    return result


def transform_item_review_stats(payload, item):
    nlp_type = payload["batch"]["nlp_type"]
    item_tmp = {}
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["request_id"] = payload["batch"]["request_id"]
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["nlp_type"] = nlp_type
    item_tmp["review_stats_date"] = parse(item["__review_stats_date__"]).isoformat()
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]
    # actual data, file after preprocess must have these fields
    item_tmp["total_ratings"] = item.get("total_ratings")  # one of these 2 or both
    item_tmp["total_reviews"] = item.get("total_reviews")  # one of these 2 or both
    item_tmp["average_rating"] = item["average_rating"]
    item_tmp["data_version"] = payload["batch"]["data_version"]

    return [item_tmp]


def transform_item_voe_review(payload, item, field):
    nlp_type = payload["batch"]["nlp_type"]
    result = []

    item_tmp = {}
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["review_id"] = item["review_id"]
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["request_id"] = payload["batch"]["request_id"]
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["nlp_type"] = nlp_type
    item_tmp["user_name"] = item["user_name"]
    item_tmp["review_date"] = parse(item["date"]).isoformat()
    item_tmp["review_language"] = item["trans_language_" + field]
    item_tmp["review"] = item["review"]
    item_tmp["trans_review"] = item["trans_" + field]
    item_tmp["trans_status"] = item["trans_status_" + field]
    item_tmp["rating"] = item["rating"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]

    if item.get("review_country"):
        item_tmp["review_country"] = item.get("review_country")
    else:
        item_tmp["review_country"] = "Unknown"

    if item.get("review_country_code"):
        item_tmp["review_country_code"] = item.get("review_country_code")
    else:
        item_tmp["review_country_code"] = "Unknown"

    # for parent review and technical type mapping
    item_tmp["parent_review_id"] = item["parent_review_id"]
    item_tmp["technical_type"] = item["technical_type"]

    # review hash, generated from preprocessing step for check if review is existed
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["review_hash"] = item["review_hash"]

    item_tmp["data_version"] = payload["batch"]["data_version"]

    result.append(item_tmp)

    return result


def transform_func_review_stats(payload, item):
    try:
        return transform_item_review_stats(payload, item)
    except Exception as error:
        logger.exception(f"[App.transform_func_review_stats] : {error}")
        return []


def transform_func_voc(payload, item):
    try:
        return transform_item(payload, item)
    except Exception as error:
        logger.error(f"[App.transform_func] : {error}")
        return []
    except:
        logger.error(f"[App.transform_func]")
        return []


def transform_func_voe_review(payload, item):
    return transform_item_voe_review(payload, item, "review")


def transform_func_voe_overview(payload, item):
    result = []

    item_tmp = {}
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["min_fte"] = item["min_fte"]
    item_tmp["max_fte"] = item["max_fte"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["source_id"] = payload["batch"]["source_id"]

    # overview_hash, generated from preprocessing step
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["overview_hash"] = item["overview_hash"]
    item_tmp["data_version"] = payload["batch"]["data_version"]

    result.append(item_tmp)

    return result


role_seniorities_suffix = ["Assistant", "Intern", "Trainee", "Entry"]


role_seniorities = {
    "Manager": "Manager",
    "Senior Manager": "Senior Manager",
    "Director": "Director",
    "Vice President": "Vice President",
    "Senior Vice President": "Senior Vice President",
    "CEO": "C Level",
    "COO": "C Level",
    "CMO": "C Level",
    "CFO": "C Level",
    "CIO": "C Level",
    "CTO": "C Level",
    "CCO": "C Level",
    "CLO": "C Level",
    "CXO": "C Level",
    "CPO": "C Level",
    "CHRO": "C Level",
    "CAE": "C Level",
    "CRO": "C Level",
    "CVO": "C Level",
    "CSO": "C Level",
    "CGO": "C Level",
    "CAO": "C Level",
    "CDO": "C Level",
    # regex pattern to match any "Chief Whatever Officer(s)"
    r"Chief\b[A-Za-z ]*\bOfficer(?:s){0,1}": "C Level",
    "Head Of": "Head",
    "Dept Head": "Head",
    "Department Head": "Head",
}


def map_role_seniority_linkedin(item):
    return map_role_seniority_indeed(item)


def map_role_seniority_ambitionbox(item):
    return map_role_seniority_indeed(item)


def map_role_seniority_indeed(item):
    if "trans_job_name" not in item:
        raise Exception("trans_job_name does not exist")

    non_manager = "Non-Manager".lower()
    wb_pattern = r"\b"  # word_boundary pattern, to add to regex

    # assistant-anything is NOT manager level
    for item_suffix in role_seniorities_suffix:
        _assistant_pattern = wb_pattern + item_suffix + wb_pattern
        if re.search(_assistant_pattern, item["trans_job_name"], re.IGNORECASE):
            return non_manager

    for role, level in role_seniorities.items():
        # not assistant => match in manager/c-level dict
        _role_pattern = wb_pattern + role + wb_pattern
        if re.search(_role_pattern, item["trans_job_name"], re.IGNORECASE):
            return level.lower()

    # if all check fails => non-manager
    return non_manager


def map_role_seniority_glassdoor(item):
    if "trans_job_name" not in item:
        raise Exception("trans_job_name does not exist")

    if "trans_job_function" not in item:
        raise Exception("trans_job_function does not exist")

    non_manager = "Non-Manager".lower()
    wb_pattern = r"\b"  # word_boundary pattern, to add to regex

    # assistant-anything is NOT manager level
    for item_suffix in role_seniorities_suffix:
        _assistant_pattern = wb_pattern + item_suffix + wb_pattern
        if any(
            [
                re.search(_assistant_pattern, item["trans_job_name"], re.IGNORECASE),
                re.search(
                    _assistant_pattern, item["trans_job_function"], re.IGNORECASE
                ),
            ]
        ):
            return non_manager

    for role, level in role_seniorities.items():
        # not assistant => match in manager/c-level dict
        _role_pattern = wb_pattern + role + wb_pattern
        if any(
            [
                re.search(_role_pattern, item["trans_job_name"], re.IGNORECASE),
                re.search(_role_pattern, item["trans_job_function"], re.IGNORECASE),
            ]
        ):
            return level.lower()

    return non_manager


def map_role_seniority_csv(item):
    return map_role_seniority_glassdoor(item)


def extract_role_seniority(payload, item):
    try:
        if payload["batch"]["source_code"].lower() == "glassdoor":
            return map_role_seniority_glassdoor(item)
        elif payload["batch"]["source_code"].lower() == "indeed":
            return map_role_seniority_indeed(item)
        elif payload["batch"]["source_code"].lower() == "linkedin":
            return map_role_seniority_linkedin(item)
        elif payload["batch"]["source_code"].lower() == "ambitionbox":
            return map_role_seniority_ambitionbox(item)
        elif payload["batch"]["source_type"].lower() == "csv":
            return map_role_seniority_csv(item)
        else:
            raise Exception(
                f"Not support source_code={payload['batch']['source_code']}"
            )
    except Exception as error:
        logger.exception(f"Cannot extract role_seniority")
        return None


def transform_func_voe_job(payload, item):
    result = []

    item_tmp = {}
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["job_id"] = item["job_id"]
    item_tmp["job_name"] = item["trans_job_name"]
    item_tmp["job_function"] = item["trans_job_function"]
    item_tmp["job_type"] = item["trans_job_type"]
    item_tmp["posted_date"] = item["posted_date"]
    item_tmp["job_country"] = item["job_country"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["request_id"] = payload["step_detail"]["request_id"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]

    # extract role seniority from trans_job_name and trans_job_function
    item_tmp["role_seniority"] = extract_role_seniority(payload, item)

    # get job hash for check job exists with company_datasource
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["job_hash"] = item["job_hash"]

    # is_classified for job function classification
    item_tmp["is_classified"] = False

    # trigger version of this item
    item_tmp["data_version"] = payload["batch"]["data_version"]

    result.append(item_tmp)

    return result


def transform_func(payload, item):
    return etl_util_v2.transform_func(
        payload,
        item,
        # web
        transform_func_voc_review=transform_func_voc,
        transform_func_voe_review=transform_func_voe_review,
        transform_func_voe_job=transform_func_voe_job,
        transform_func_voe_overview=transform_func_voe_overview,
        transform_func_review_stats=transform_func_review_stats,
        # coresignal
        transform_func_coresignal_stats=transform_func_coresignal_stats,
        transform_func_coresignal_employees=transform_func_coresignal_employees,
    )


def _load_data_into_mongodb(payload, items):
    def _get_mongodb_collection(payload):
        if (
            payload["batch"]["nlp_type"].lower()
            == etl_const.Meta_NLPType.VOC.value.lower()
        ):
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.REVIEW.value.lower()
            ):
                return config.MONGODB_VOC_REVIEW_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
            ):
                return config.MONGODB_VOC_REVIEW_STATS_COLLECTION

        elif (
            payload["batch"]["nlp_type"].lower()
            == etl_const.Meta_NLPType.VOE.value.lower()
        ):
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.REVIEW.value.lower()
            ):
                return config.MONGODB_VOE_REVIEW_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
            ):
                return config.MONGODB_VOE_REVIEW_STATS_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.OVERVIEW.value.lower()
            ):
                return config.MONGODB_VOE_OVERVIEW_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.JOB.value.lower()
            ):
                return config.MONGODB_VOE_JOB_COLLECTION
            
        elif payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.HR.value.lower():
            if payload['step_detail']['meta_data']['data_type'].lower() == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower():
                return config.MONGODB_HR_CORESIGNAL_STATS_COLLECTION
            if payload['step_detail']['meta_data']['data_type'].lower() == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower():
                return config.MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION

        elif (
            payload["batch"]["nlp_type"].lower()
            == etl_const.Meta_NLPType.HR.value.lower()
        ):
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_STATS_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION

        elif (
            payload["batch"]["nlp_type"].lower()
            == etl_const.Meta_NLPType.HR.value.lower()
        ):
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_STATS_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION

        elif (
            payload["batch"]["nlp_type"].lower()
            == etl_const.Meta_NLPType.HR.value.lower()
        ):
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_STATS_COLLECTION
            if (
                payload["step_detail"]["meta_data"]["data_type"].lower()
                == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
            ):
                return config.MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION

        return None

    # NOTE: Route coresignal employees to a special loadfunction
    if (
        payload["step_detail"]["meta_data"]["data_type"].lower()
        == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
    ):
        return _load_coresignal_linkedin_employee(
            payload, items, mongodb_db=db, logger=logger
        )

    else:
        mongodb_collection = _get_mongodb_collection(payload)
        logger.info(f"Load file to MongoDB: total_row={len(items)}")
        chunks = common_util.split_list(items, config.LOAD_CHUNK_SIZE)

        inserted_items_count = 0
        has_exception = False
        inserted_errors = []
        for chunk in chunks:
            try:
                logger.info(
                    f"Start loading {len(chunk)} items into {mongodb_collection}"
                )
                results = db[mongodb_collection].insert_many(
                    documents=chunk, ordered=False
                )
                inserted_items_count += len(results.inserted_ids)

            except pm_errors.BulkWriteError as exc:
                inserted_items_count += exc.details.get("nInserted", 0)
                has_exception = True
                inserted_errors.append(str(exc))
                logger.exception(
                    f"Error while loading {len(chunk)} items into {mongodb_collection} - Error: {exc}"
                )

            finally:
                logger.info(f"End loading {len(chunk)} items into {mongodb_collection}")

        return inserted_items_count, has_exception, inserted_errors


def _load_into_bq_company_datasource_statistic(payload, item_count):
    data_type = payload["step_detail"]["meta_data"]["data_type"].lower()

    # get statistic table name
    if payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOE.value.lower():
        bq_statistic_table = config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS
    elif (
        payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOC.value.lower()
    ):
        bq_statistic_table = config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS
    else:
        bq_statistic_table = config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS

    # get appropriate data_version from data_type
    data_version = payload["batch"]["data_version"]

    row = {
        "request_id": payload["request"]["request_id"],
        "company_datasource_id": payload["batch"]["company_datasource_id"],
        "step_detail_id": payload["step_detail"]["step_detail_id"],
        "created_at": payload["step_detail"]["created_at"],
        "company_id": payload["batch"]["company_id"],
        "company_name": payload["batch"]["company_name"],
        "source_id": payload["batch"]["source_id"],
        "source_name": payload["batch"]["source_name"],
        "batch_id": payload["batch"]["batch_id"],
        "num_reviews": item_count,
        "data_version": data_version,
        "data_type": data_type,
    }

    bq_client = bigquery.Client()
    bq_errors = bq_client.insert_rows_json(bq_statistic_table, [row])
    if bq_errors:
        logger.error(
            f"Encountered errors while inserting into {bq_statistic_table}: {bq_errors}"
        )


def load_func(payload, transformed_items):
    try:
        if len(transformed_items) == 0:
            inserted_items_count = 0
        else:
            (
                inserted_items_count,
                has_exception,
                inserted_errors,
            ) = _load_data_into_mongodb(payload, transformed_items)

        _load_into_bq_company_datasource_statistic(payload, inserted_items_count)

    except Exception as error:
        logger.exception(
            f"[Error_Load]: Error while loading data - Step detail: {payload['step_detail']['step_detail_id']} - {error}"
        )
        raise error


def handle_task(payload):
    try:
        etlHandler.gcp_bq_table_voc = (
            None  # NOTE: this is obsolete and needs to be removed
        )
        etlHandler.handle(payload)

    except Exception as error:
        logger.exception(f"handle_task: {error}")
        raise error


if __name__ == "__main__":
    etlHandler = etl_util_v2.EtlHandler.create_for_load(
        logger=logger,
        gcp_project_id=config.GCP_PROJECT_ID,
        gcp_bucket_name=config.GCP_STORAGE_BUCKET,
        gcp_bq_table_voc=config.GCP_BQ_TABLE_VOC,
        gcp_topic_after=config.GCP_PUBSUB_TOPIC_AFTER,
        gcp_topic_progress=config.GCP_PUBSUB_TOPIC_PROGRESS,
        gcp_topic=config.GCP_PUBSUB_TOPIC,
        src_step_type="translate",
        dst_step_type="load",
        src_dir=config.SRC_DIR,
        transform_func=transform_func,
        load_func=load_func,
        progress_threshold=config.PROGRESS_THRESHOLD,
        thread_enable=1,
        thread_count=10,
    )

    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    # Begin consume task
    pubsub_util.subscribe(
        logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task
    )
