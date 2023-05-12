import requests
import time
import json
from datetime import datetime
import sys

sys.path.append("../")

from google.cloud import bigquery

import config
import helpers
import mongodb_helpers
import bigquery_helpers
from core import pubsub_util
from core.logger import init_logger

logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def handle_load_case_study_data(payload, logger):
    """
    Handle loading data from MongoDB to BigQuery:
    - for VOC or VOE reviews:
        - at each review, split each nlp result item into a separate review
    - for VOE job, overview: map as-is
    - load stream to bigquery by batch
    """
    # parse payload
    request_id = payload["request_id"]
    case_study_id = payload["case_study_id"]
    case_study_payload = payload["payload"]
    case_study_name = case_study_payload["case_study_name"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    postprocess_type = payload["postprocess"]["postprocess_type"]
    case_study_action = case_study_payload["action"]
    cs_data_version_changes = payload["case_study_data_version_changes"]

    # if translation service not one of the options below (or not specified),
    # then only load reviews with native lang as Eng
    translation_services = "Google Translation"
    if case_study_payload.get("translation_service") in ("Google Translation"):
        load_eng_only = False
    else:
        load_eng_only = True

    logger.info(
        f"[handle_load_case_study_data] "
        f"translation_service: {case_study_payload.get('translation_service')} - "
        f"load_eng_only: {load_eng_only}"
    )

    # extract cs_version_changes to dict
    cs_data_version_dict = {}
    for dvc in cs_data_version_changes:
        _id = dvc["company_datasource_id"]
        temp_dict = {}
        if dvc["data_type"] == "review":
            temp_dict["from_version"] = dvc["previous_data_version"]
            temp_dict["to_version"] = dvc["new_data_version"]
        elif dvc["data_type"] == "job":
            temp_dict["from_version_job"] = dvc["previous_data_version"]
            temp_dict["to_version_job"] = dvc["new_data_version"]
        elif dvc["data_type"] == "review_stats":
            temp_dict["from_version_review_stats"] = dvc["previous_data_version"]
            temp_dict["to_version_review_stats"] = dvc["new_data_version"]
        elif dvc["data_type"] == "all":
            temp_dict["from_version"] = dvc["previous_data_version"]
            temp_dict["to_version"] = dvc["new_data_version"]
            temp_dict["from_version_job"] = dvc["previous_data_version"]
            temp_dict["to_version_job"] = dvc["new_data_version"]

        # for HRA
        # NOTE: this is actually not used, just for completeness, 
        # so that it fits the flows for VoC and VoE.
        elif dvc["data_type"] == "coresignal_stats": 
            temp_dict["from_version"] = dvc["previous_data_version"]
            temp_dict["to_version"] = dvc["new_data_version"]
        elif dvc["data_type"] == "coresignal_employees": 
            temp_dict["from_version"] = dvc["previous_data_version"]
            temp_dict["to_version"] = dvc["new_data_version"]
        
        if _id in cs_data_version_dict:
            cs_data_version_dict[_id].update(temp_dict)
        else:
            cs_data_version_dict[_id] = temp_dict

    # process for each company_datasource, since each can have different data_version
    num_company = len(case_study_payload["company_datasources"])
    company_datasource_ids = [
        int(item["company_datasource_id"]) 
        for item in case_study_payload["company_datasources"]
    ]
    target_company_datasource_id = [
        int(item["company_datasource_id"]) 
        for item in case_study_payload["company_datasources"]
        if (item["is_target"] == True)
    ][0]
    event = "finish"
    error_text = ""
    start_time = datetime.utcnow()
    # prepare clients
    mongodb = mongodb_helpers.create_mongodb_client()
    bq_client = bigquery.Client()

    i = 0
    for company_datasource_dict in case_study_payload["company_datasources"]:
        company_name = company_datasource_dict["company_name"]
        company_start_time = datetime.utcnow()
        i += 1
        try:
            # extract renlp and version info
            company_datasource_id = company_datasource_dict["company_datasource_id"]
            from_version = cs_data_version_dict[company_datasource_id].get(
                "from_version", 0
            ) if cs_data_version_dict else 0
            to_version = cs_data_version_dict[company_datasource_id].get(
                "to_version", 0
            ) if cs_data_version_dict else 0

            from_version_review_stats = cs_data_version_dict[company_datasource_id].get(
                "from_version_review_stats", 0
            ) if cs_data_version_dict else 0
            to_version_review_stats = cs_data_version_dict[company_datasource_id].get(
                "to_version_review_stats", 0
            ) if cs_data_version_dict else 0

            from_version_job = cs_data_version_dict[company_datasource_id].get(
                "from_version_job", 0
            ) if cs_data_version_dict else 0
            to_version_job = cs_data_version_dict[company_datasource_id].get(
                "to_version_job", 0
            ) if cs_data_version_dict else 0
            # extract nlp info for MC
            nlp_type = company_datasource_dict["nlp_type"]
            nlp_pack = company_datasource_dict["nlp_pack"]

            # extract data_types and URLs
            urls = company_datasource_dict["urls"]
            urls_dict = {d["type"]: d["url"] for d in urls}
            logger.info(f"[handle_load_case_study_data] available URLs: {urls_dict}")

            logger.info(
                f"[handle_load_case_study_data] inserting data "
                f"for case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id} - "
                f"from_version {from_version} - "
                f"to_version {to_version} - "
                f"from_version_job {from_version_job} - "
                f"to_version_job {to_version_job} - "
                f"nlp_type {nlp_type} - "
                f"nlp_pack {nlp_pack}"
            )
            if nlp_type.strip().lower() == "voc":
                # only review and review_stats
                if to_version_review_stats:
                    helpers.handle_load_case_study_data_voc_review_stats(
                        case_study_id=case_study_id,
                        company_datasource_id=company_datasource_id,
                        company_name=company_name,
                        url=urls_dict["review"],
                        data_version_review_stats=to_version_review_stats,
                        mongodb=mongodb,
                        bq_client=bq_client,
                        logger=logger,
                    )
                helpers.handle_load_case_study_data_voc_review(
                    request_id=request_id,
                    case_study_id=case_study_id,
                    case_study_name=case_study_name,
                    company_datasource_id=company_datasource_id,
                    company_name=company_name,
                    url=urls_dict["review"],
                    to_version=to_version,
                    nlp_pack=nlp_pack,
                    from_version=from_version,
                    load_eng_only=load_eng_only,
                    mongodb=mongodb,
                    bq_client=bq_client,
                    logger=logger,
                )

            elif nlp_type.strip().lower() == "voe":
                # review_stats
                if to_version_review_stats:
                    helpers.handle_load_case_study_data_voe_review_stats(
                        case_study_id=case_study_id,
                        company_datasource_id=company_datasource_id,
                        company_name=company_name,
                        url=urls_dict["review"],
                        data_version_review_stats=to_version_review_stats,
                        mongodb=mongodb,
                        bq_client=bq_client,
                        logger=logger,
                    )
                # reviews
                helpers.handle_load_case_study_data_voe_review(
                    request_id=request_id,
                    case_study_id=case_study_id,
                    case_study_name=case_study_name,
                    company_datasource_id=company_datasource_id,
                    company_name=company_name,
                    url=urls_dict["review"],
                    to_version=to_version,
                    nlp_pack=nlp_pack,
                    from_version=from_version,
                    load_eng_only=load_eng_only,
                    mongodb=mongodb,
                    bq_client=bq_client,
                    logger=logger,
                )
                # jobs
                helpers.handle_load_case_study_data_voe_job(
                    request_id=request_id,
                    case_study_id=case_study_id,
                    case_study_name=case_study_name,
                    company_datasource_id=company_datasource_id,
                    company_name=company_name,
                    url=urls_dict["job"],
                    to_version_job=to_version_job,
                    from_version_job=from_version_job,
                    mongodb=mongodb,
                    bq_client=bq_client,
                    logger=logger,
                )
                # overview company info
                helpers.handle_load_case_study_data_voe_overview(
                    request_id=request_id,
                    case_study_id=case_study_id,
                    case_study_name=case_study_name,
                    company_datasource_id=company_datasource_id,
                    company_name=company_name,
                    url=urls_dict["overview"],
                    data_version_job=to_version_job,
                    mongodb=mongodb,
                    bq_client=bq_client,
                    logger=logger,
                )
                # log
                logger.info(
                    "[handle_load_case_study_data] insert for "
                    f"case_study_id {case_study_id} - "
                    f"company_datasource_id {company_datasource_id} "
                    f"finished in {datetime.utcnow() - company_start_time}"
                )

            elif nlp_type.strip().lower() == "hra":
                helpers.handle_load_case_study_hra(
                    case_study_id=case_study_id,
                    case_study_name=case_study_name,
                    company_datasource_ids=company_datasource_ids,
                    target_company_datasource_id=target_company_datasource_id,
                    company_datasource=company_datasource_dict,
                    mongodb=mongodb,
                    logger=logger
                )

            else:
                raise Exception(
                    f"[handle_load_case_study_data] invalid nlp_type: {nlp_type}"
                )

        except Exception as exception:
            event = "fail"
            error_text = f"{exception.__class__.__name__}: {exception.args}"
            logger.exception(
                "[handle_load_case_study_data] error happened while inserting data for "
                f"case_study_id {case_study_id} - "
                f"company_datasource_id {company_datasource_id}, will skip"
            )

        finally:
            # send progress to internal progress
            progress = min(i / num_company, 0.95)
            helpers.update_worker_progress(
                payload=payload, progress=progress, logger=logger
            )

    logger.info(
        "[handle_load_case_study_data] data insert for "
        f"case_study_id {case_study_id} finished in {datetime.utcnow() - start_time}"
    )
    return event, error_text


def handle_clone_case_study_data(payload, logger):
    """
    DEPRECATED
    Handle cloning data
    """
    # parse payload
    case_study_id = payload["case_study_id"]
    case_study_payload = payload["payload"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    postprocess_type = payload["postprocess"]["postprocess_type"]
    case_study_action = case_study_payload["action"]
    clone_from_cs_id = case_study_payload["clone_from_cs_id"]

    # extract info
    event = "finish"
    error_text = ""
    start_time = datetime.utcnow()
    nlp_type = case_study_payload["company_datasources"][0]["nlp_type"]

    logger.info(
        f"[handle_load_case_study_clone] cloning "
        f"case_study_id {clone_from_cs_id} to {case_study_id}"
    )

    try:
        if nlp_type.strip().lower() == "voc":
            bigquery_helpers.clone_voc(
                from_case_study_id=clone_from_cs_id,
                to_case_study_id=case_study_id,
                logger=logger,
            )

        elif nlp_type.strip().lower() == "voe":
            bigquery_helpers.clone_voe(
                from_case_study_id=clone_from_cs_id,
                to_case_study_id=case_study_id,
                logger=logger,
            )
            bigquery_helpers.clone_voe_company(
                from_case_study_id=clone_from_cs_id,
                to_case_study_id=case_study_id,
                logger=logger,
            )
            bigquery_helpers.clone_voe_job(
                from_case_study_id=clone_from_cs_id,
                to_case_study_id=case_study_id,
                logger=logger,
            )
        else:
            raise Exception(
                f"[handle_load_case_study_clone] invalid nlp_type: {nlp_type}"
            )

        logger.info(
            f"[handle_load_case_study_clone] finished cloning "
            f"case_study_id {clone_from_cs_id} to {case_study_id}"
        )
    except Exception as exception:
        event = "fail"
        error_text = f"{exception.__class__.__name__}: {exception.args}"
        logger.exception(
            "[handle_load_case_study_clone] error happened while cloning BQ data for "
            f"case_study_id {case_study_id}"
        )

    helpers.update_worker_progress(payload=payload, progress=0.95, logger=logger)
    return event, error_text


def handle_task(payload):
    """
    Main handler
    """
    # parse payload
    request_id = payload["request_id"]
    case_study_id = payload["case_study_id"]
    case_study_payload = payload["payload"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    postprocess_type = payload["postprocess"]["postprocess_type"]
    postprocess_object = payload["postprocess"]
    case_study_action = case_study_payload["action"]

    if case_study_action in ("sync", "update"):
        event, error_text = handle_load_case_study_data(payload, logger)
    else:
        event = "fail"
        error_text = f"Invalid case_study_action: {case_study_action}"

    # finish, announce to after task
    time.sleep(10)
    aftertask_payload = {
        "type": postprocess_type,
        "event": event,
        "case_study_id": case_study_id,
        "request_id": request_id,
        "postprocess_id": postprocess_id,
        "error": error_text,
        "postprocess": postprocess_object,
    }
    pubsub_util.publish(
        gcp_product_id=config.GCP_PROJECT_ID,
        topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
        payload=aftertask_payload,
        logger=logger,
    )


if __name__ == "__main__":
    pubsub_util.subscribe(
        logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task
    )
