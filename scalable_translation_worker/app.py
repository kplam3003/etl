import sys
import csv

sys.path.append("../")
# extend limit of CSV file
csv.field_size_limit(sys.maxsize)

import config
from core import pubsub_util, logger, etl_util_v2
import requests
import time
from langdetect import detect
from datetime import datetime, timedelta
from typing import Union, List


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
DELIMITER = "\t"
TIMEOUT = 300


def is_valid_item(item, field):
    review_date = item[field].timestamp()
    date_3_year_ago = (
        datetime.now() - timedelta(days=config.YEAR_AGO * 365)
    ).timestamp()
    if review_date >= date_3_year_ago:
        return True
    else:
        return False


def _translate_api(text: List[str], ref_id: Union[None, str]):
    """
    Caller should handle size of `text` itself.
    """
    if not ref_id:
        ref_id = str(hash(frozenset(text)))

    logger.info(f'Begin translate: ref_id={ref_id}, text="{text}"')
    for wait in [0.5, 1, 3, 5]:
        try:
            res = requests.post(
                config.TRANSLATE_API,
                headers={"Authorization": config.TRANSLATE_SECRET, "x-ref-id": ref_id},
                json={"text": text},
                timeout=TIMEOUT,
            )

            if res.status_code >= 300:
                logger.info(
                    f"Unable to translate. Status: {res.status_code}, reason: {res.content}"
                )
                logger.info(f"Retry again after {wait} secs")
                time.sleep(wait)
                logger.info(f"Retrying...")
                continue

            logger.info(
                f"*** End translate: ref_id={res.headers['x-ref-id']}, content=\"{res.text}\""
            )
            return {
                "status": True,
                "data": res.json(),
                "ref_id": res.headers["x-ref-id"],
            }
        except requests.exceptions.Timeout as error:
            logger.error(f"[App.transform.translate_api] : {error}")
            time.sleep(wait)
            continue

    logger.info(f"*** End translate: Unable to translate after retrying few times ***")
    return {"status": False}


def _detect_lang(text: str) -> Union[str, None]:
    try:
        return detect(text)
    except:
        # no text feature | empty | others
        return None


def transform_review_items(payload, items):
    field_translate = "review"
    is_translation = payload["request"]["is_translation"]
    for review in items:
        review["language"] = _detect_lang(review[field_translate])

    if is_translation == False:
        for review in items:
            review["trans_review"] = None
            review["trans_status"] = False
            review["is_translation"] = is_translation
            review["raw_trans_review"] = None
            review["ref_id"] = None
        return items

    transformed_ids = []
    empty_reviews = [item for item in items if item[field_translate] == ""]
    for review in empty_reviews:
        review["trans_review"] = review[field_translate]
        review["trans_status"] = False
        review["is_translation"] = is_translation
        review["raw_trans_review"] = None
        review["ref_id"] = None
    transformed_ids.extend([item["review_id"] for item in empty_reviews])

    en_reviews = [
        item
        for item in items
        if item["review_id"] not in transformed_ids and item["language"] == "en"
    ]
    for review in en_reviews:
        review["trans_review"] = review[field_translate]
        review["trans_status"] = True
        review["is_translation"] = True
        review["raw_trans_review"] = None
        review["ref_id"] = None
    transformed_ids.extend([item["review_id"] for item in en_reviews])

    translate_reviews = [
        item for item in items if item["review_id"] not in transformed_ids
    ]
    translate_text = [item[field_translate] for item in translate_reviews]
    translate_response = _translate_api(text=translate_text, ref_id=None)
    translate_status: bool = translate_response["status"]
    for idx, review in enumerate(translate_reviews):
        trans_review = None
        ref_id = None
        if translate_status == True:
            trans_review = translate_response["data"][idx]["text"]
            ref_id = review["review_id"]
        review["trans_review"] = trans_review
        review["trans_status"] = translate_status
        review["is_translation"] = is_translation
        review["raw_trans_review"] = None
        review["ref_id"] = ref_id

    return empty_reviews + en_reviews + translate_reviews


def map_translate_result(
    item, field, trans_value, lang, status, is_translation, raw, ref_id
):
    if trans_value:
        item["trans_" + field] = trans_value.lower()
    else:
        item["trans_" + field] = trans_value
    item["trans_language_" + field] = lang
    item["trans_status_" + field] = status
    item["trans_is_translation_" + field] = is_translation
    item["raw_trans_" + field] = raw
    item["ref_id_" + field] = ref_id
    return item


def is_job_type_english(text):
    if text in {
        "full_time",
        "part_time",
        "fulltime",
        "parttime",
        "full-time",
        "part-time",
        "internship",
        "temporary",
        "contract",
        "permanet",
        "volunteer",
        "apprenticeship",
    }:
        return True
    return False


def standardize_job_type(job_type):
    return job_type.replace("-", "").replace("_", "")


def translate_items(payload, items, ref_id_field, fields):
    is_translation: bool = payload["request"]["is_translation"]
    for field in fields:
        langs = {
            item[ref_id_field]: "en"
            if is_job_type_english(item[field])
            else _detect_lang(item[field])
            for item in items
        }
        transformed_ids = []

        # No translate payload
        if is_translation == False:
            items = [
                map_translate_result(
                    item,
                    field,
                    None,
                    langs[item[ref_id_field]],
                    False,
                    is_translation,
                    None,
                    None,
                )
                for item in items
            ]
            continue

        # Content of translate field is empty
        empty_items = [
            item
            for item in items
            if (not item[field] or str(item[field]).strip() == "")
        ]
        empty_items = [
            map_translate_result(
                item,
                field,
                None,
                langs[item[ref_id_field]],
                False,
                is_translation,
                None,
                None,
            )
            for item in empty_items
        ]
        transformed_ids.extend([item[ref_id_field] for item in empty_items])

        # Content of translate field is in English
        en_items = [
            item
            for item in items
            if (
                item[ref_id_field] not in transformed_ids
                and langs[item[ref_id_field]] == "en"
            )
        ]
        en_items = [
            map_translate_result(item, field, item[field], "en", True, True, None, None)
            for item in en_items
        ]
        transformed_ids.extend([item[ref_id_field] for item in en_items])

        # Items to translate
        translate_items = [
            item for item in items if (item[ref_id_field] not in transformed_ids)
        ]
        translate_text = [item[field] for item in translate_items]
        translate_response = _translate_api(text=translate_text, ref_id=None)
        translate_status: bool = translate_response["status"]
        if translate_status:
            translate_items = [
                map_translate_result(
                    item,
                    field,
                    translate_response["data"][idx]["text"],
                    langs[item[ref_id_field]],
                    True,
                    is_translation,
                    None,
                    item[ref_id_field],
                )
                for idx, item in enumerate(translate_items)
            ]
        else:
            translate_items = [
                map_translate_result(
                    item,
                    field,
                    None,
                    langs[item[ref_id_field]],
                    False,
                    is_translation,
                    None,
                    None,
                )
                for item in translate_items
            ]

        # Update `items` with translation result of current `field`
        items = empty_items + en_items + translate_items

    return items


def transform_func_voe_review(payload, items):
    try:
        items = translate_items(payload, items, "review_id", ["review"])
        for item in items:
            item["trans_date"] = item["date"]
        return items
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_review] : {error}")
        return []
    except:
        logger.exception(f"[App.transform_func_voe_review]")
        return []


def transform_func_voe_overview(payload, items):
    return items


def transform_func_voe_job(payload, items):
    try:
        # translate job type, job function, and job name
        # job name is to extract role seniority later
        items = translate_items(
            payload, items, "job_id_job_type", ["job_type", "job_function", "job_name"]
        )
        for item in items:
            item["job_type"] = standardize_job_type(item["job_type"])
            item["job_function"] = standardize_job_type(item["job_function"])
            item["job_name"] = standardize_job_type(item["job_name"])

        return items
    except Exception as error:
        logger.error(f"[App.transform_func_voe_job] : {error}")
        return []
    except:
        logger.error(f"[App.transform_func_voe_job]")
        return []


def transform_func_voc_review(payload, items):
    try:
        return transform_review_items(payload, items)
    except Exception as error:
        logger.error(f"[App.transform_func_voc] : {error}")
        return []
    except:
        logger.error(f"[App.transform_func_voc]")
        return []


def transform_func_review_stats(payload, items):
    return items


def transform_func_coresignal_stats(payload, items):
    return items


def transform_func_coresignal_employees(payload, items):
    return items


def transform_func(payload, items):
    return etl_util_v2.transform_func(
        payload,
        items,
        # web scraping
        transform_func_voc_review=transform_func_voc_review,
        transform_func_voe_review=transform_func_voe_review,
        transform_func_voe_job=transform_func_voe_job,
        transform_func_voe_overview=transform_func_voe_overview,
        transform_func_review_stats=transform_func_review_stats,
        # coresignal
        transform_func_coresignal_stats=transform_func_coresignal_stats,
        transform_func_coresignal_employees=transform_func_coresignal_employees,
    )


def handle_task(payload, set_terminator=None):
    logger.info("-------------------------[HANDLE_TASK] Begin")

    try:
        etlHandler.handle(payload)
    except Exception as error:
        logger.info(f"handle_task: {error}")
    logger.info("-------------------------[HANDLE_TASK] End")


if __name__ == "__main__":
    etlHandler = etl_util_v2.EtlHandler.create_for_transform(
        logger=logger,
        gcp_project_id=config.GCP_PROJECT_ID,
        gcp_bucket_name=config.GCP_STORAGE_BUCKET,
        gcp_topic_after=config.GCP_PUBSUB_TOPIC_AFTER,
        gcp_topic_progress=config.GCP_PUBSUB_TOPIC_PROGRESS,
        gcp_topic=config.GCP_PUBSUB_TOPIC,
        src_step_type="preprocess",
        dst_step_type="translate",
        src_dir=config.SRC_DIR,
        dst_dir=config.DST_DIR,
        transform_func=transform_func,
        load_func=None,
        progress_threshold=config.PROGRESS_THRESHOLD,
        transform_batch_size=config.TRANSFORM_BATCH_SIZE,
        thread_enable=1,
        thread_count=10,
    )

    # Begin consume task
    pubsub_util.subscribe(
        logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task
    )
