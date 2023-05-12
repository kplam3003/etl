import bson
import sys

sys.path.append("../")

import logging
from typing import List
from google.cloud import bigquery
from pymongo.database import Database

import config
import mongodb_helpers
from core import pubsub_util
from hra_coresignal import (
    _load_coresignal_stats,
    _load_coresignal_company_datasource,
    _load_coresignal_employees,
    _load_coresignal_employees_experiences,
    _load_coresignal_employees_education,
)


def convert_string_to_bool(s):
    if s.strip().lower() == "true":
        return True
    else:
        return False


def prepare_review_doc_for_insert(
    review_document_dict,
    case_study_id,
    case_study_name,
    company_name,
    url,
    request_id,
    nlp_pack,
):
    """
    Unnest/flatten nlp result in document from MongoDB
    """
    single_nlp_result = review_document_dict.pop("single_nlp_result")
    for key, value in single_nlp_result.items():
        single_nlp_result[key] = value
    # add new fields to original review document
    review_document_dict.update(single_nlp_result)  # nlp results
    review_document_dict.update(
        {  # fields that is not available from mongo document
            "id": 0,
            "case_study_id": case_study_id,
            "request_id": request_id,
            "case_study_name": case_study_name,
            "company_name": company_name,
            "nlp_pack": nlp_pack,
            "url": url,
        }
    )
    # convert string bool to bool
    # review_document_dict["trans_status"] = convert_string_to_bool(review_document_dict["trans_status"])

    return review_document_dict


def update_worker_progress(payload, progress, logger):
    """
    Helper function to publish worker's progress

    """
    case_study_id = payload["case_study_id"]
    request_id = payload["request_id"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    worker_type = payload["postprocess"]["postprocess_type"]

    progress_payload = {
        "type": worker_type,
        "event": "progress",
        "request_id": request_id,
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_id,
        "progress": progress,
    }
    pubsub_util.publish(
        logger=logger,
        gcp_product_id=config.GCP_PROJECT_ID,
        topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
        payload=progress_payload,
    )
    return True


def handle_load_case_study_data_voc_review(
    request_id,
    case_study_id,
    case_study_name,
    company_datasource_id,
    company_name,
    url,
    to_version,
    nlp_pack,
    mongodb,
    bq_client,
    logger,
    load_eng_only=False,
    from_version=0,
):
    mongo_cursor = mongodb_helpers.get_voc_reviews_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        to_version=to_version,
        nlp_pack=nlp_pack,
        from_version=from_version,
        load_eng_only=load_eng_only,
    )
    # load to BQ by batch
    total_rows = 0
    total_error_rows = 0
    for batch in mongo_cursor:
        try:
            batch = bson.decode_all(batch)
            batch_error_rows = 0
            # process each batch, first unnest nlp result and add case study id
            unnested_batch = list(
                map(
                    lambda b: prepare_review_doc_for_insert(
                        review_document_dict=b,
                        case_study_id=case_study_id,
                        case_study_name=case_study_name,
                        company_name=company_name,
                        url=url,
                        request_id=request_id,
                        nlp_pack=nlp_pack,
                    ),
                    batch,
                )
            )
            batch_rows = len(unnested_batch)
            total_rows += batch_rows
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                table=config.GCP_BQ_TABLE_VOC,
                json_rows=unnested_batch,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voc_reviews] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"num_rows {batch_rows} - "
                    f"num_errors {len(errors)} - "
                    f"first error: {errors[0]} - "
                    f"first row: {unnested_batch[0]}"
                )
                total_error_rows += len(errors)
                batch_error_rows += len(errors)

            logger.info(
                f"[load_voc_reviews] case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id}: "
                f"{batch_rows - batch_error_rows} rows inserted successfully - "
                f"first row: {unnested_batch[0]}"
            )

        except Exception as exception:
            logger.exception(
                f"[load_voc_reviews] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"first item {batch[0]}"
            )

    # announce finish
    logger.info(
        f"[load_voc_reviews] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, total {total_rows - total_error_rows} rows inserted"
    )


def handle_load_case_study_data_voe_review(
    request_id,
    case_study_id,
    case_study_name,
    company_datasource_id,
    company_name,
    url,
    to_version,
    nlp_pack,
    mongodb,
    bq_client,
    logger,
    load_eng_only=False,
    from_version=0,
):
    mongo_cursor = mongodb_helpers.get_voe_reviews_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        to_version=to_version,
        nlp_pack=nlp_pack,
        from_version=from_version,
        load_eng_only=load_eng_only,
    )
    # load to BQ by batch
    total_rows = 0
    total_error_rows = 0
    for batch in mongo_cursor:
        try:
            batch = bson.decode_all(batch)
            batch_error_rows = 0
            # process each batch, first unnest nlp result and add case study id
            unnested_batch = list(
                map(
                    lambda b: prepare_review_doc_for_insert(
                        review_document_dict=b,
                        case_study_id=case_study_id,
                        case_study_name=case_study_name,
                        company_name=company_name,
                        url=url,
                        request_id=request_id,
                        nlp_pack=nlp_pack,
                    ),
                    batch,
                )
            )
            batch_rows = len(unnested_batch)
            total_rows += batch_rows
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                table=config.GCP_BQ_TABLE_VOE,
                json_rows=unnested_batch,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voc_reviews] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"num_rows {batch_rows} - "
                    f"num_errors {len(errors)} - "
                    f"first error: {errors[0]} - "
                    f"first row: {unnested_batch[0]}"
                )
                total_error_rows += len(errors)
                batch_error_rows += len(errors)

            logger.info(
                f"[load_voc_reviews] case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id}: "
                f"{batch_rows - batch_error_rows} rows inserted successfully - "
                f"first row: {unnested_batch[0]}"
            )

        except Exception as exception:
            logger.exception(
                f"[load_voc_reviews] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"first item {batch[0]}"
            )

    # announce finish
    logger.info(
        f"[load_voc_reviews] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, total {total_rows - total_error_rows} rows inserted"
    )


def handle_load_case_study_data_voe_job(
    request_id,
    case_study_id,
    case_study_name,
    company_datasource_id,
    company_name,
    url,
    to_version_job,
    mongodb,
    bq_client,
    logger,
    from_version_job=0,
):
    mongo_cursor = mongodb_helpers.get_voe_jobs_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        to_version_job=to_version_job,
        from_version_job=from_version_job,
    )
    # load to BQ by batch
    total_rows = 0
    total_error_rows = 0
    for batch in mongo_cursor:
        try:
            decoded_batch = bson.decode_all(batch)
            batch_error_rows = 0
            # process each batch of jobs, first add case study id and request id into document

            list(
                map(  # dict.update is performed in-place, and returns None => decoded_batch is updated after this
                    lambda b: b.update(
                        {
                            "case_study_id": case_study_id,
                            "request_id": request_id,
                            "company_name": company_name,
                            "url": url,
                        }
                    ),
                    decoded_batch,
                )
            )

            batch_rows = len(decoded_batch)
            total_rows += batch_rows
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                table=config.GCP_BQ_TABLE_VOE_JOB,
                json_rows=decoded_batch,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voe_jobs] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"num_rows {batch_rows} - "
                    f"num_errors {len(errors)} - "
                    f"first error: {errors[0]} - "
                    f"first row: {decoded_batch[0]}"
                )
                total_error_rows += len(errors)
                batch_error_rows += len(errors)

            logger.info(
                f"[load_voe_jobs] case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id}: "
                f"{batch_rows - batch_error_rows} rows inserted successfully - "
                f"first row: {decoded_batch[0]}"
            )

        except Exception as exception:
            logger.exception(
                f"[load_voe_jobs] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"first item {decoded_batch[0]}"
            )

    # announce finish
    logger.info(
        f"[load_voe_jobs] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, total {total_rows - total_error_rows} rows inserted"
    )


def handle_load_case_study_data_voe_overview(
    request_id,
    case_study_id,
    case_study_name,
    company_datasource_id,
    company_name,
    url,
    data_version_job,
    mongodb,
    bq_client,
    logger,
):
    mongo_cursor = mongodb_helpers.get_voe_overview_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        data_version_job=data_version_job,
    )
    # load to BQ by batch
    i = 0
    for doc in mongo_cursor:
        try:
            # overview for one version has only 1 row (hopefully)
            # so just add case study id into doc and insert to BQ table
            doc.update(
                {
                    "case_study_id": case_study_id,
                    "company_name": company_name,
                    "url": url,
                }
            )
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                json_rows=[doc],
                table=config.GCP_BQ_TABLE_VOE_COMPANY,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voe_overview] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"first error: {errors[0]} - "
                    f"item: {doc}"
                )
            else:
                logger.info(
                    f"[load_voe_overview] case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id}: "
                    f"1 rows inserted successfully: "
                    f"{doc}"
                )
                i += 1

        except Exception as exception:
            logger.exception(
                f"[load_voe_overview] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"item {doc}"
            )

    # announce finish
    logger.info(
        f"[load_voe_overview] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, {i} rows inserted"
    )


def handle_load_case_study_data_voc_review_stats(
    case_study_id,
    company_datasource_id,
    company_name,
    url,
    data_version_review_stats,
    mongodb,
    bq_client,
    logger,
):
    mongo_cursor = mongodb_helpers.get_voc_review_stats_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        data_version_review_stats=data_version_review_stats,
    )
    # load to BQ by batch
    i = 0
    for doc in mongo_cursor:
        try:
            # review_stats for one version has only 1 row (hopefully)
            # so just add case study id into doc and insert to BQ table
            doc.update(
                {
                    "case_study_id": case_study_id,
                    "company_name": company_name,
                    "url": url,
                }
            )
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                json_rows=[doc],
                table=config.GCP_BQ_TABLE_VOC_REVIEW_STATS,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voc_review_stats] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"first error: {errors[0]} - "
                    f"item: {doc}"
                )
            else:
                logger.info(
                    f"[load_voc_review_stats] case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id}: "
                    f"1 rows inserted successfully: "
                    f"{doc}"
                )
                i += 1

        except Exception as exception:
            logger.exception(
                f"[load_voc_review_stats] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"item {doc}"
            )

    # announce finish
    logger.info(
        f"[load_voc_review_stats] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, {i} rows inserted"
    )


def handle_load_case_study_data_voe_review_stats(
    case_study_id,
    company_datasource_id,
    company_name,
    url,
    data_version_review_stats,
    mongodb,
    bq_client,
    logger,
):
    mongo_cursor = mongodb_helpers.get_voe_review_stats_for_company_datasource(
        db=mongodb,
        company_datasource_id=company_datasource_id,
        data_version_review_stats=data_version_review_stats,
    )
    # load to BQ by batch
    i = 0
    for doc in mongo_cursor:
        try:
            # review_stats for one version has only 1 row (hopefully)
            # so just add case study id into doc and insert to BQ table
            doc.update(
                {
                    "case_study_id": case_study_id,
                    "company_name": company_name,
                    "url": url,
                }
            )
            # stream to BQ
            bq_client = bigquery.Client()
            errors = bq_client.insert_rows_json(
                json_rows=[doc],
                table=config.GCP_BQ_TABLE_VOE_REVIEW_STATS,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            if errors:
                logger.error(
                    f"[load_voe_review_stats] error happened while inserting "
                    f"data for case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} - "
                    f"first error: {errors[0]} - "
                    f"item: {doc}"
                )
            else:
                logger.info(
                    f"[load_voe_review_stats] case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id}: "
                    f"1 rows inserted successfully: "
                    f"{doc}"
                )
                i += 1

        except Exception as exception:
            logger.exception(
                f"[load_voe_review_stats] error happened while inserting batch "
                f"data for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"item {doc}"
            )

    # announce finish
    logger.info(
        f"[load_voe_review_stats] case_study_id {case_study_id} - "
        f"company_datasource_id {company_datasource_id}: "
        f"BQ insert finished, {i} rows inserted"
    )


def handle_load_case_study_hra(
    case_study_id: int,
    case_study_name: str,
    company_datasource_ids: List[int],
    target_company_datasource_id: int,
    company_datasource: dict,
    mongodb: Database,
    logger: logging.Logger,
) -> None:
    """
    Load data from MongoDB to BigQuery for a `company_datasource_id` of a `case_study_id`.
    Table `coresignal_stats` -> `staging.coresignal_stats`
    Table `coresignal_company_datasource` -> `staging.coresignal_company_datasource`
    Table `coresignal_employees` -> `staging.coresignal_employees|coresignal_employees_experiences|coresignal_employees_education`
    """
    # Check & Load `coresignal_stats`
    _load_coresignal_stats(
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        company_datasource_id=int(company_datasource["company_datasource_id"]),
        nlp_pack=company_datasource["nlp_pack"],
        mongodb=mongodb,
    )

    # Check & Load `coresignal_company_datasource`
    coresignal_member_ids = _load_coresignal_company_datasource(
        case_study_id=case_study_id,
        company_datasource=company_datasource,
        mongodb=mongodb,
    )
    if len(coresignal_member_ids) == 0:
        return None

    # Check & Load `coresignal_employees`
    loaded_rows = _load_coresignal_employees(
        mongodb=mongodb,
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        company_datasource_id=int(company_datasource["company_datasource_id"]),
        coresignal_member_ids=coresignal_member_ids,
    )
    msg = f"Load (case_study_id: {case_study_id}, company_datasource_id={company_datasource['company_datasource_id']}): {loaded_rows} rows"
    logger.info(msg)
    print(msg)

    # Check & Load `coresignal_employees_experiences`
    loaded_rows = _load_coresignal_employees_experiences(
        mongodb=mongodb,
        case_study_id=case_study_id,
        company_datasource_id=int(company_datasource["company_datasource_id"]),
        company_datasource_ids=company_datasource_ids,
        target_company_datasource_id=target_company_datasource_id,
        coresignal_member_ids=coresignal_member_ids,
    )
    msg = f"Load experiences (case_study_id: {case_study_id}, company_datasource_id={company_datasource['company_datasource_id']}): {loaded_rows} rows"
    logger.info(msg)
    print(msg)

    # Check & Load `coresignal_employees_education`
    loaded_rows = _load_coresignal_employees_education(
        mongodb=mongodb,
        case_study_id=case_study_id,
        company_datasource_id=int(company_datasource["company_datasource_id"]),
        coresignal_member_ids=coresignal_member_ids,
    )
    msg = f"Load education (case_study_id: {case_study_id}, company_datasource_id={company_datasource['company_datasource_id']}): {loaded_rows} rows"
    logger.info(msg)
    print(msg)

    return None
