import re
import json
import os
import random
import datetime as dt
from itertools import product


import pandas as pd
from google.cloud import storage
from google.cloud import pubsub_v1
from sqlalchemy.orm.attributes import flag_modified
from google_play_scraper import Sort, reviews, app

import utils
import config
from database import ETLStepDetail, ETLCompanyBatch, ETLStep, auto_session
from helpers import log_crawl_stats


logger = utils.load_logger()

DELIMITER = "\t"

# Google Play countries
GOOGLEPLAY_COUNTRIES = [
    "us",
    "al",
    "dz",
    "ao",
    "ag",
    "ar",
    "am",
    "aw",
    "au",
    "at",
    "az",
    "bs",
    "bh",
    "bd",
    "by",
    "be",
    "bz",
    "bj",
    "bo",
    "ba",
    "bw",
    "br",
    "bg",
    "bf",
    "kh",
    "cm",
    "ca",
    "cl",
    "co",
    "cr",
    "hr",
    "cy",
    "cz",
    "dk",
    "do",
    "ec",
    "eg",
    "sv",
    "ee",
    "fj",
    "fi",
    "fr",
    "ga",
    "ge",
    "de",
    "gh",
    "gi",
    "gr",
    "gt",
    "gw",
    "ht",
    "hn",
    "hk",
    "hu",
    "is",
    "in",
    "id",
    "iq",
    "ie",
    "il",
    "it",
    "jm",
    "jp",
    "jo",
    "kz",
    "ke",
    "kw",
    "kg",
    "lv",
    "lb",
    "li",
    "lt",
    "lu",
    "mk",
    "my",
    "ml",
    "mt",
    "mu",
    "mx",
    "md",
    "mc",
    "ma",
    "mz",
    "mm",
    "na",
    "np",
    "nl",
    "nz",
    "ni",
    "ng",
    "ng",
    "no",
    "om",
    "pk",
    "pa",
    "pg",
    "py",
    "pe",
    "ph",
    "pl",
    "pt",
    "qa",
    "ro",
    "ru",
    "rw",
    "sm",
    "sa",
    "sn",
    "rs",
    "sg",
    "sk",
    "si",
    "za",
    "kr",
    "es",
    "lk",
    "se",
    "ch",
    "tw",
    "tj",
    "tz",
    "th",
    "tg",
    "tt",
    "tn",
    "tr",
    "tm",
    "ug",
    "ua",
    "ae",
    "gb",
    "us",
    "uy",
    "uz",
    "ve",
    "vn",
    "ye",
    "zm",
    "zw",
]

GOOGLEPLAY_SUPPORTED_LANGS = [
    ("Afrikaans", "af"),
    ("Amharic", "am"),
    ("Bulgarian", "bg"),
    ("Catalan", "ca"),
    ("Chinese", "zh"),
    ("Croatian", "hr"),
    ("Czech", "cs"),
    ("Danish", "da"),
    ("Dutch", "nl"),
    ("English", "en"),
    ("Estonian", "et"),
    ("Filipino", "fil"),
    ("Finnish", "fi"),
    ("French", "fr"),
    ("German", "de"),
    ("Greek", "el"),
    ("Hebrew", "he"),
    ("Hindi", "hi"),
    ("Hungarian", "hu"),
    ("Icelandic", "is"),
    ("Indonesian", "id"),
    ("Italian", "it"),
    ("Japanese", "ja"),
    ("Korean", "ko"),
    ("Latvian", "lv"),
    ("Lithuanian", "lt"),
    ("Malay", "ms"),
    ("Norwegian", "no"),
    ("Polish", "pl"),
    ("Portuguese", "pt"),
    ("Romanian", "ro"),
    ("Russian", "ru"),
    ("Serbian", "sr"),
    ("Slovak", "sk"),
    ("Slovenian", "sl"),
    ("Spanish", "es"),
    ("Swahili", "sw"),
    ("Swedish", "sv"),
    ("Thai", "th"),
    ("Turkish", "tr"),
    ("Ukrainian", "uk"),
    ("Vietnamese", "vi"),
    ("Zulu", "zu"),
    ("Hongkong", "zh-HK"),
    ("Taiwan", "zh-TW"),
]


def mkdirs_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


@utils.retry(5)
def request_get(*args, **kwargs):
    return reviews(*args, **kwargs)


def fetch_app_data(app_id):
    """Takes in a app_id, returns the number of reviews"""
    app_data = app(app_id, country="al", lang="af")
    actual_ratings = app_data.get("ratings", 0)
    logger.info(f"Checked first comb : [al, af] : {actual_ratings} ratings")
    # if data is found here, no need to proceed further
    if actual_ratings != 0:
        return app_data

    # iterate over a random combination to search for ratings
    language_codes = list(map(lambda item: item[1], GOOGLEPLAY_SUPPORTED_LANGS))
    choices = random.choices(list(product(GOOGLEPLAY_COUNTRIES, language_codes)), k=10)
    for i, comb in enumerate(choices):
        try:
            app_data = app(app_id, lang=comb[1], country=comb[0])
            current_ratings = app_data.get("ratings", 0)
            actual_ratings = max(actual_ratings, current_ratings)
            logger.info(f"Checked comb {i}: {comb} : {current_ratings} ratings")
            if current_ratings != 0:
                return app_data

        except Exception as e:
            logger.exception(f"Error: {e}")
            return {}


def crawl_review_stats(url, step_detail, batch=None, step=None, session=None):
    logger.info(
        f"Crawling apple store review_stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )
    total_pages = 1
    page_exceptions = []
    error_pages = []
    try:
        match = re.search(r"id=([^&]+)", url)
        app_id = match.group(1)
        data = fetch_app_data(app_id)
        # map to output dict
        output_dict = {}
        output_dict["score"] = data.get("score")
        output_dict["ratings"] = data.get("ratings")
        output_dict.update(
            zip(
                [f"{i}_star{'s' if i > 1 else ''}" for i in range(1, 6)],
                data.get("histogram", [None] * 5),
            )
        )

        # save crawled item count
        step_detail.item_count = 1
        session.commit()

        # write local csv
        mkdirs_if_not_exists(config.OUTPUT_DIR)
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        df = pd.DataFrame([output_dict])
        df.to_csv(output_file, sep=DELIMITER, index=False)

        # upload csv to GCS
        *path, filename = output_file.split("/")

        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        logger.info(f"Uploaded file to gcs: {dst_file}")

        # Log crawl statistics to BigQuery
        log_crawl_stats(
            batch=batch,
            step_detail=step_detail,
            item_count=int(output_dict["ratings"])
        )

    except Exception as e:
        logger.exception(f"Unable to crawl url: {url}")
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, total_pages, error_pages


def crawl(url, step_detail, continue_token, batch=None, step=None, lang="en"):
    logger.info(f"Crawling url: {url}, step_detail: {step_detail.step_detail_id}")
    match = re.search(r"id=([^&]+)", url)
    app_id = match.group(1)

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

        # Crawl
        params = {"continuation_token": continue_token} if continue_token else {}
        result = []
        try:
            result, continue_token = request_get(
                app_id, lang=lang, sort=Sort.NEWEST, count=200, **params
            )

            result = list(map(lambda item: {**item, "lang": lang}, result))
            total_comments = [*total_comments, *result]
            logger.info(f"Crawled {len(total_comments)} review items.")

            if len(result) < 200:
                logger.info(f"No more reviews on lang: {lang}")
                break

        except Exception as e:
            logger.exception(f"Unable to crawl url: {url}")
            page_exceptions.append(e)
            error_pages.append(index + 1)

    mkdirs_if_not_exists(config.OUTPUT_DIR)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    return (
        page_exceptions,
        total_pages,
        error_pages,
        output_file,
        continue_token,
        total_comments,
    )


@auto_session
def execute_task(task, session=None):
    # get payload
    url = task.get("url")
    ids = task.get("ids")
    batch_id = task.get("batch_id")
    batch = session.query(ETLCompanyBatch).get(batch_id)
    meta_data = batch.meta_data

    # crawler info
    data_type = meta_data.get("data_type", "review") if meta_data else "review"
    continue_token = None
    langs = list(map(lambda item: item[1], SUPPORTED_LANGS))
    cur_lang = langs.pop()

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        step_detail.lang = cur_lang
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        # Crawl data from source website
        try:
            if data_type == "review":
                total_comments = []
                total_page_exceptions = []
                total_error_pages = []
                total_pages = 0
                while len(langs) >= 0:
                    (
                        page_exceptions,
                        these_pages,
                        error_pages,
                        src_file,
                        continue_token,
                        comments,
                    ) = crawl(
                        url,
                        step_detail,
                        continue_token,
                        batch=batch,
                        step=step,
                        lang=cur_lang,
                    )

                    meta_data = step_detail.meta_data or {}
                    paging_info = meta_data.get("paging_info", {})
                    by_langs = paging_info.get("by_langs", {})
                    by_langs[cur_lang] = by_langs.get(cur_lang, 0) + len(comments)
                    paging_info["by_langs"] = by_langs
                    meta_data["paging_info"] = paging_info
                    step_detail.meta_data = {**meta_data}
                    flag_modified(step_detail, "meta_data")
                    session.commit()

                    total_comments = [*total_comments, *comments]
                    total_page_exceptions.extend(page_exceptions)
                    total_error_pages.extend(error_pages)
                    total_pages += these_pages
                    if len(total_comments) >= config.ROWS_PER_STEP:
                        # Still have comments to crawl on current language
                        # break out to process current chunk of comments
                        break
                    else:
                        # No more comments to crawl on this language, try next language
                        if len(langs) == 0:
                            # Also no more lang to process, break out
                            break
                        # Try on next lang
                        cur_lang = langs.pop()
                        continue_token = None

                step_detail.item_count = len(total_comments)
                session.commit()

                # Upload crawled file to google cloud storage
                *path, filename = src_file.split("/")
                df = pd.DataFrame(total_comments)
                df.drop_duplicates(inplace=True)
                df.to_csv(src_file, index=False, sep=DELIMITER)

                dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
                upload_google_storage(src_file, dst_file)
                logger.info(f"Uploaded file to gcs: {dst_file}")

                # Log crawl statistics to BigQuery
                log_crawl_stats(
                    batch=batch,
                    step_detail=step_detail,
                    item_count=len(total_comments)
                )

            # handle review_stats data_type which is simpler
            elif data_type == "review_stats":
                page_exceptions, these_pages, error_pages = crawl_review_stats(
                    url, step_detail, batch=batch, step=step, session=session
                )
            else:
                raise Exception(f"Not supported crawler = {data_type}")

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
                    page_exceptions=total_page_exceptions,
                    total_pages=total_pages,
                    error_pages=total_error_pages,
                    session=session,
                    logger=logger,
                )

        except Exception as e:
            logger.exception(
                f"Unable to crawl step detail {step_detail.step_detail_id}"
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


## Google Play supported languages
## See more at https://support.google.com/googleplay/android-developer/table/4419860

SUPPORTED_LANGS = [
    ("Afrikaans", "af"),
    ("Amharic", "am"),
    ("Bulgarian", "bg"),
    ("Catalan", "ca"),
    ("Chinese", "zh"),
    ("Croatian", "hr"),
    ("Czech", "cs"),
    ("Danish", "da"),
    ("Dutch", "nl"),
    ("English", "en"),
    ("Estonian", "et"),
    ("Filipino", "fil"),
    ("Finnish", "fi"),
    ("French", "fr"),
    ("German", "de"),
    ("Greek", "el"),
    ("Hebrew", "he"),
    ("Hindi", "hi"),
    ("Hungarian", "hu"),
    ("Icelandic", "is"),
    ("Indonesian", "id"),
    ("Italian", "it"),
    ("Japanese", "ja"),
    ("Korean", "ko"),
    ("Latvian", "lv"),
    ("Lithuanian", "lt"),
    ("Malay", "ms"),
    ("Norwegian", "no"),
    ("Polish", "pl"),
    ("Portuguese", "pt"),
    ("Romanian", "ro"),
    ("Russian", "ru"),
    ("Serbian", "sr"),
    ("Slovak", "sk"),
    ("Slovenian", "sl"),
    ("Spanish", "es"),
    ("Swahili", "sw"),
    ("Swedish", "sv"),
    ("Thai", "th"),
    ("Turkish", "tr"),
    ("Ukrainian", "uk"),
    ("Vietnamese", "vi"),
    ("Zulu", "zu"),
    ("Hongkong", "zh-HK"),
    ("Taiwan", "zh-TW"),
]
