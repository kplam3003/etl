import csv
import os
import sys

sys.path.append("../")
# extend limit of CSV file
csv.field_size_limit(sys.maxsize)

import config
from core import pubsub_util, logger, etl_util_v2, common_util, etl_const, gcs_util
from datetime import datetime, timedelta
import json
from dateparser import parse
import base64
import hashlib
import re
import pycountry
import helpers
from copy import deepcopy
from pymongo import MongoClient

from review_stats import transform_func_review_stats
from coresignal import (
    transform_func_coresignal_stats,
    transform_func_coresignal_employees
)


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
DELIMITER = "\t"
TIMEOUT = 5

GLASSDOOR_JOB_COUNTRY_MAPPING = {
    "Czech Republic": "Czechia",
    "South Korea": "Korea, Republic of",
    "Russia": "Russian Federation",
    "Taiwan": "Taiwan, Province of China",
    "Vietnam": "Viet Nam",
}

JOB_TYPE_MAPPING = {
    "full_time": "full-time",
    "part_time": "part-time",
    "contractor": "contract",
    "permanent": "permanent",
    "new-grad": "new-grad",
    "fulltime": "full-time",
    "full-time job": "full-time",
    "full time": "full-time",
    "parttime": "part-time",
    "part-time job": "part-time",
    "part time": "part-time",
    "halftime": "part-time",
    "part-time employment": "part-time",
    "contracted": "contract",
    "by contract": "contract",
    "contract employee": "contract",
    "intern": "internship",
    "training": "temporary",
    "internship / practice": "temporary",
    "scholarship / practices": "temporary",
    "scholarship / internships": "temporary",
    "internship": "temporary",
    "temporal": "temporary",
    "temporary job": "temporary",
    "apprentice/trainee": "apprenticeship",
    "commission": "commission",
    "business commission": "commission",
    "permanent employment": "permanent",
    "other": "other",
    "temporary": "temporary",
}


etlHandler = None

client = None
db = None


def scale_rating(old_value, old_min, old_max, new_min=0.0, new_max=5.0):
    """
    Scale rating from other scales to [0.0, 5.0] given min and max value of old scale
    """
    new_value = ((old_value - old_min) / (old_max - old_min)) * (
        new_max - new_min
    ) + new_min
    return new_value


def generate_unique_id(text):
    encoded_text = f"{base64.b64encode(text.encode('utf-8'))}"
    hashed_text = hashlib.sha512(encoded_text.encode("utf-8")).hexdigest()
    return hashed_text


def transform_google_play(payload, item):
    try:
        item["__review_date__"] = parse(item["at"])
        unique_str = generate_unique_id(
            item["userName"] + str(payload["batch"]["company_id"])
        )
        item["review_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["at"]
            + "_"
            + item["reviewId"]
        )
        item["review"] = item["content"].replace("\n", "").strip()

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])

        item["rating"] = item["score"]
        item["user_name"] = ""
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_mouthshut(payload, item):
    try:
        item["__review_date__"] = parse(item["create_at"])
        item["user_name"] = item["user_name"]
        unique_str = generate_unique_id(
            item["user_name"] + str(payload["batch"]["company_id"])
        )
        review_id = (
            payload["batch"]["source_name"] + "_" + unique_str + "_" + item["create_at"]
        )
        item["review_id"] = review_id
        item["rating"] = item["rating"]
        # default parameters
        item[
            "parent_review_id"
        ] = review_id  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"
        # extract country and codes, non-trivial
        country, code = helpers.search_country_mouthshut(item.get("country"))
        item["review_country"] = country
        item["review_country_code"] = code

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_capterra(payload, item):
    try:
        results = []

        item["__review_date__"] = parse(item["writtenOn"])
        reviewer = json.loads(item["reviewer"], strict=False)
        item["user_name"] = reviewer["fullName"]
        unique_str = generate_unique_id(
            item["user_name"] + str(payload["batch"]["company_id"])
        )
        review_id = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["writtenOn"]
            + "_"
            + item["reviewId"]
        )

        item["review_id"] = review_id
        item["rating"] = item["overallRating"]
        item["user_name"] = ""
        # default parameters
        item[
            "parent_review_id"
        ] = review_id  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        item["nlp_results"] = {"__NLP_PACKS__": []}

        item["review_hash"] = generate_unique_id(
            item["review_id"]
            + item.get("generalComments").replace("\n", "").strip()
            + item.get("prosText").replace("\n", "").strip()
            + item.get("consText").replace("\n", "").strip()
            + item.get("generalComments").replace("\n", "").strip()
        )

        # overall review
        if item.get("generalComments"):
            item_overall = item.copy()
            item_overall["review_id"] = item["parent_review_id"] + "_overall"
            item_overall["review"] = item["generalComments"].replace("\n", "").strip()
            item_overall["technical_type"] = "overall"
            item_overall["review_hash"] += "_" + item_overall["technical_type"]

            if item_overall.get("review"):
                results.append(item_overall)

        # pros
        if item.get("prosText"):
            item_pros = item.copy()
            item_pros["review_id"] = item_pros["parent_review_id"] + "_pros"
            item_pros["review"] = item_pros["prosText"].replace("\n", "").strip()
            item_pros["technical_type"] = "pros"

            item_pros["review_hash"] += "_" + item_pros["technical_type"]

            if item_pros.get("review"):
                results.append(item_pros)

        # cons
        if item.get("consText"):
            item_cons = item.copy()
            item_cons["review_id"] = item_cons["parent_review_id"] + "_cons"
            item_cons["review"] = item_cons["consText"].replace("\n", "").strip()
            item_cons["technical_type"] = "cons"

            item_cons["review_hash"] += "_" + item_cons["technical_type"]

            if item_cons.get("review"):
                results.append(item_cons)

        # switch_reasons
        if item.get("switchingReasons"):
            item_switch = item.copy()
            item_switch["review_id"] = item["parent_review_id"] + "_switch_reasons"
            item_switch["review"] = item["switchingReasons"].replace("\n", "").strip()
            item_switch["technical_type"] = "switch_reasons"

            item_switch["review_hash"] += "_" + item_switch["technical_type"]

            if item_switch.get("review"):
                results.append(item_switch)

        return results

    except Exception as error:
        logger.exception(f"[App.transform] : error={error}, item={item}")
        return results


def transform_apple_store(payload, item):
    try:
        item["__review_date__"] = parse(item["date"])
        item["review_id"] = item["id"]
        item["review"] = item["review"].replace("\n", "").strip()
        item["user_name"] = item["userName"]
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_trustpilot(payload, item):
    try:
        item["__review_date__"] = parse(item["comment_time"])
        unique_str = generate_unique_id(
            item["consumerName"] + str(payload["batch"]["company_id"])
        )
        item["review_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["comment_time"]
        )
        item["review"] = item["reviewBody"].replace("\n", "").strip()
        item["rating"] = float(item["rating"][0])
        item["user_name"] = ""

        if item.get("country"):
            item["review_country"] = pycountry.countries.search_fuzzy(item["country"])[
                0
            ].name
            item["review_country_code"] = pycountry.countries.search_fuzzy(
                item["country"]
            )[0].alpha_2

        else:
            item["review_country"] = None
            item["review_country_code"] = None

        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_g2(payload, item):
    try:
        results = []

        # prepare parents item
        unique_str = generate_unique_id(str(payload["batch"]["company_id"]))
        # this id can sometimes be float, or in float format with .0,
        # and may cause duplication
        # => proactively remove ALL possible decimal part
        existing_id = re.sub(r"\.\d*", "", str(item["id"]))
        review_id = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["date"]
            + "_"
            + existing_id
        )

        item["user_name"] = ""
        item["__review_date__"] = parse(item["date"])
        item["review_id"] = review_id
        # default parameters for cases where there is error
        item[
            "parent_review_id"
        ] = review_id  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        # process rating
        # G2 rating range is 0-10 while other sources' range is 1-5
        try:
            old_rating = float(item["rating_value"])
            item["rating"] = float(
                scale_rating(
                    old_value=old_rating,
                    old_min=0.0,
                    old_max=10.0,
                    new_min=1.0,
                    new_max=5.0,
                )
            )
        except Exception as error:
            logger.warning(
                f"[App.transform] : G2 rating value is {item['rating_value']}"
            )
            item["rating"] = None

        # process review text
        review_text = item["review_text"].replace("\n", ". ").strip()
        deduped_review_text = helpers.safe_dedup_g2_luminati_review(
            review_text, logger=logger
        )  # dedup
        technical_review_dict = helpers.split_g2_review(
            deduped_review_text, logger=logger
        )  # split into technical types
        item["review"] = deduped_review_text

        item["review_hash"] = generate_unique_id(review_id + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        # return the original item if cannot be splitted
        if len(technical_review_dict) == 0:
            return [item]

        # return the original item if cannot be splitted
        if deduped_review_text.split() == "":
            return []

        # split parent item into one item for each technical type
        for category, text in technical_review_dict.items():
            if text.replace("\n", " ").strip() == "":
                continue

            item_tmp = deepcopy(item)
            item_tmp["review_id"] = review_id + "_" + category
            item_tmp["review"] = text.replace("\n", " ").strip()
            item_tmp["technical_type"] = category

            item_tmp["review_hash"] += "_" + item_tmp["technical_type"]

            results.append(item_tmp)

        return results

    except Exception as error:
        logger.exception(f"[App.transform] : {error.__class__.__name__}")
        return results


def transform_gartner(payload, item):
    try:
        results = []

        item["__review_date__"] = parse(item["formattedReviewDate"])
        unique_str = generate_unique_id(str(payload["batch"]["company_id"]))
        review_id = (
            payload["batch"]["source_name"] + "_" + unique_str + "_" + item["reviewId"]
        )

        item["review_id"] = review_id
        item["review"] = item["reviewSummary"].replace("\n", " ").strip()
        item["rating"] = float(item["reviewRating"])
        item["user_name"] = ""
        # default parameters
        item[
            "parent_review_id"
        ] = review_id  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        # extract technical reviews
        # stop if parent review has no review text
        if not item["review"]:
            return []

        # overall review, required
        if item.get("review-summary", None):
            item_overall = item.copy()
            item_overall["review_id"] = item["parent_review_id"] + "_overall"
            item_overall["review"] = item["review-summary"].replace("\n", "").strip()
            item_overall["technical_type"] = "overall"
            item_overall["review_hash"] += "_" + item_overall["technical_type"]
            if item_overall["review"]:
                results.append(item_overall)

        # pros, required
        if item.get("lessonslearned-like-most", None):
            item_pros = item.copy()
            item_pros["review_id"] = item_pros["parent_review_id"] + "_pros"
            item_pros["review"] = (
                item_pros["lessonslearned-like-most"].replace("\n", "").strip()
            )
            item_pros["technical_type"] = "pros"
            item_pros["review_hash"] += "_" + item_pros["technical_type"]
            if item_pros["review"]:
                results.append(item_pros)

        # cons, required
        if item.get("lessonslearned-dislike-most", None):
            item_cons = item.copy()
            item_cons["review_id"] = item_cons["parent_review_id"] + "_cons"
            item_cons["review"] = (
                item_cons["lessonslearned-dislike-most"].replace("\n", "").strip()
            )
            item_cons["technical_type"] = "cons"
            item_cons["review_hash"] += "_" + item_cons["technical_type"]
            if item_cons["review"]:
                results.append(item_cons)

        # business problems solved, optional
        if item.get("business-problem-solved", None):
            item_solved = item.copy()
            item_solved["review_id"] = (
                item_solved["parent_review_id"] + "_problems_solved"
            )
            item_solved["review"] = (
                item_solved["business-problem-solved"].replace("\n", "").strip()
            )
            item_solved["technical_type"] = "problems_solved"
            item_solved["review_hash"] += "_" + item_solved["technical_type"]
            if item_solved["review"]:
                results.append(item_solved)

        # recommendations, optional
        if item.get("lessonslearned-advice", None):
            item_recommend = item.copy()
            item_recommend["review_id"] = (
                item_recommend["parent_review_id"] + "_recommendations"
            )
            item_recommend["review"] = (
                item_recommend["lessonslearned-advice"].replace("\n", "").strip()
            )
            item_recommend["technical_type"] = "recommendations"
            item_recommend["review_hash"] += "_" + item_recommend["technical_type"]
            if item_recommend["review"]:
                results.append(item_recommend)

        # reasons for switching/purchase, required. There are 2 fields to be extracted.
        if item.get("why-purchase-s24", None) or item.get(
            "why-purchase-s24-other", None
        ):
            item_reasons = item.copy()
            item_reasons["review_id"] = (
                item_reasons["parent_review_id"] + "_switch_reasons"
            )
            item_reasons["review"] = ". ".join(
                [
                    item_reasons.get("why-purchase-s24", "").replace("\n", "").strip(),
                    item_reasons.get("why-purchase-s24-other", "")
                    .replace("\n", "")
                    .strip(),
                ]
            )
            item_reasons["technical_type"] = "switch_reasons"
            item_reasons["review_hash"] += "_" + item_reasons["technical_type"]
            if item_reasons.get("why-purchase-s24") or item_reasons.get(
                "why-purchase-s24-other"
            ):
                results.append(item_reasons)

        return results

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return results


def transform_reddit(payload, item):
    try:
        item["__review_date__"] = parse(str(item["created_utc"]))
        unique_str = generate_unique_id(
            str(item["author_fullname"]) + str(payload["batch"]["company_id"])
        )
        item["review_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + str(item["created_utc"])
        )

        # Submission has review with key selftext,
        # while comment has review with key body
        item_review = item["selftext"]
        if not item_review:
            item_review = item["body"]
        item["review"] = str(item_review).replace("\n", "").strip()

        review_id = generate_unique_id(item["review"] + str(item["created_utc"]))
        item["review_id"] = review_id

        item["rating"] = None
        item["user_name"] = str(item["author_fullname"])
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"

        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_csv(payload, item):
    try:
        item["__review_date__"] = parse(item["review_at"])
        item["review"] = item["review"].replace("\n", " ").replace("<br/>", ".").strip()
        item["rating"] = float(item["rating"])
        item["user_name"] = ""
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"
        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])
        item["nlp_results"] = {"__NLP_PACKS__": []}

        if not item["review"]:
            return []

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def get_result_before_filter(payload, item):
    if (
        "source_type" in payload["batch"]
        and payload["batch"]["source_type"]
        and payload["batch"]["source_type"].lower() == "csv"
    ):
        return transform_csv(payload, item)
    if payload["batch"]["source_code"].lower() == "GooglePlay".lower():
        return transform_google_play(payload, item)
    if payload["batch"]["source_code"].lower() == "Capterra".lower():
        return transform_capterra(payload, item)
    if payload["batch"]["source_code"].lower() == "AppleStore".lower():
        return transform_apple_store(payload, item)
    if payload["batch"]["source_code"].lower() == "Trustpilot".lower():
        return transform_trustpilot(payload, item)
    if payload["batch"]["source_code"].lower() == "G2".lower():
        return transform_g2(payload, item)
    if payload["batch"]["source_code"].lower() == "gartner":
        return transform_gartner(payload, item)
    if payload["batch"]["source_code"].lower() == "reddit":
        return transform_reddit(payload, item)
    if payload["batch"]["source_code"].lower() == "mouthshut":
        return transform_mouthshut(payload, item)

    raise Exception(f"Not support source_code: {payload['batch']['source_code']}")


def transform_func_voc(payload, item):
    try:
        transformed_items = get_result_before_filter(payload, item)
        return transformed_items

    except Exception as error:
        logger.exception(f"[App.transform_func] : {error}")
        return []


def transform_func_voe_overview_glassdoor(payload, item):
    size_buckets = re.findall(r"[-+]?\d*\.\d+|\d*\,\d+|\d+", item["size"])
    min_fte = 0
    max_fte = 0
    if len(size_buckets) == 1:
        min_fte = max_fte = size_buckets[0]
    elif len(size_buckets) == 2:
        min_fte = size_buckets[0]
        max_fte = size_buckets[1]
    else:
        raise Exception(f"item[size]={item['size']} not support")
    item["min_fte"] = min_fte
    item["max_fte"] = max_fte

    org_overview_id = "_".join(list(map(str, item.values())))
    item["overview_hash"] = generate_unique_id(org_overview_id)

    return [item]


def transform_func_voe_overview_indeed(payload, item):
    size_buckets = item["size"].split("_")
    min_fte = 0
    max_fte = 0
    if len(size_buckets) == 3:
        min_fte = size_buckets[1]
        max_fte = size_buckets[2]
    else:
        raise Exception(f"item[size]={item['size']} not support")
    item["min_fte"] = min_fte
    item["max_fte"] = max_fte
    if item["max_fte"] == "PLUS":
        item["max_fte"] = item["min_fte"]

    org_overview_id = "_".join(list(map(str, item.values())))
    item["overview_hash"] = generate_unique_id(org_overview_id)

    return [item]


def transform_func_voe_overview_linkedin(payload, item):
    company_size = item["Company size"]
    company_size = "".join([c for c in company_size if c in "0123456789-"])
    size_buckets = company_size.split("-")

    item["min_fte"] = size_buckets[0]
    item["max_fte"] = size_buckets[-1]

    org_overview_id = "_".join(list(map(str, item.values())))
    item["overview_hash"] = generate_unique_id(org_overview_id)

    return [item]


def transform_func_voe_overview_ambitionbox(payload, item):
    company_size = item["size"]
    company_size = "".join([c for c in company_size if c in "0123456789-"])
    size_buckets = company_size.split("-")

    item["min_fte"] = size_buckets[0]
    item["max_fte"] = size_buckets[-1]

    org_overview_id = "_".join(list(map(str, item.values())))
    item["overview_hash"] = generate_unique_id(org_overview_id)

    return [item]


def transform_func_voe_overview_csv(payload, item):
    item["min_fte"] = item["min_company_size"]
    item["max_fte"] = item["max_company_size"]

    org_overview_id = "_".join(list(map(str, item.values())))
    item["overview_hash"] = generate_unique_id(org_overview_id)

    return [item]


def transform_func_voe_overview(payload, item):
    try:
        if payload.get("batch", {}).get("source_type", "").lower() == "csv":
            return transform_func_voe_overview_csv(payload, item)
        elif payload["batch"]["source_code"].lower() == "Glassdoor".lower():
            return transform_func_voe_overview_glassdoor(payload, item)
        elif payload["batch"]["source_code"].lower() == "Indeed".lower():
            return transform_func_voe_overview_indeed(payload, item)
        elif payload["batch"]["source_code"].lower() == "linkedin":
            return transform_func_voe_overview_linkedin(payload, item)
        elif payload["batch"]["source_code"].lower() == "ambitionbox":
            return transform_func_voe_overview_ambitionbox(payload, item)
        else:
            raise Exception(
                f"Not support source_code={payload['batch']['source_code']}"
            )
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_overview] : {error}")
        return []


def map_country(job_domain_country):
    try:
        return pycountry.countries.get(alpha_2=job_domain_country.upper()).name
    except Exception as error:
        logger.exception(f"[App.transform.map_country] : {error}")
        return []


def map_glassdoor_country(glassdoor_country):
    return GLASSDOOR_JOB_COUNTRY_MAPPING.get(glassdoor_country, glassdoor_country)


def map_job_type(job_type):
    return JOB_TYPE_MAPPING.get(job_type.lower(), job_type)


def transform_func_voe_job_glassdoor(payload, item):
    try:
        item["job_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + str(payload["batch"]["company_id"])
            + "_"
            + item["job_id"]
        )
        item["job_id_job_type"] = item["job_id"]
        item["job_name"] = item["title"]
        item["job_function"] = item["job_function"].lower()
        item["job_type"] = item["job_type"].lower()
        item["is_classified"] = False

        time = item.get("time") or item.get("posted")
        time = time.replace("+", "").replace("Easy Apply", "")
        try:
            item["posted_date"] = parse(time)
        except:
            m = re.search("([0-9]+)", time)
            delta = float(m.group(1))
            item["posted_date"] = datetime.now() - timedelta(days=delta)
        if not item["posted_date"]:
            raise Exception("no item['posted_date']")
            return []

        item["job_country"] = map_glassdoor_country(item["country"]).lower()
        # item['role_seniority'] = map_role_seniority_glassdoor(item).lower()
        item[
            "role_seniority"
        ] = ""  # place holder, will be replaced with value on load step

        results = []
        for ix, job_type in enumerate(item.get("job_type", "").split(", ")):
            new_item = item.copy()
            new_item["job_type"] = map_job_type(job_type).lower()
            new_item["job_id"] = f'{item.get("job_id")}_{ix}'

            new_item["job_hash"] = generate_unique_id(
                new_item["job_id"]
                + new_item["job_name"]
                + new_item["job_function"]
                + new_item["job_type"]
            )

            results.append(new_item)

        return results

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def get_posted_date(posted_days):
    try:
        posted_days_items = re.findall(r"[-+]?\d*\.\d+|\d*\,\d+|\d+", posted_days)
        return posted_days_items[0]
    except Exception as error:
        logger.exception(f"[App.get_posted_date] : {error}")
        raise error


def transform_func_voe_job_indeed(payload, item):
    try:
        item["job_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + str(payload["batch"]["company_id"])
            + "_"
            + item["job_id"]
        )
        item["job_name"] = item["title"]
        item["job_function"] = list(
            map(lambda x: x.lower() or "", json.loads(item["job_categories"]))
        ) or [""]
        item["job_type"] = list(
            map(lambda x: x.lower() or "", json.loads(item["job_types"]))
        ) or [""]
        item["job_type"] = list(set(item["job_type"]))
        item["is_classified"] = False

        time = item.get("time") or item.get("posted")
        time = time.replace("+", "")
        try:
            item["posted_date"] = parse(item["time"])
        except:
            m = re.search("([0-9]+)", time)
            delta = float(m.group(1))
            item["posted_date"] = datetime.now() - timedelta(days=delta)

        if not item["posted_date"]:
            return []

        item["job_country"] = map_country(item["country"]).lower()

        item[
            "role_seniority"
        ] = ""  # place holder, will be replaced with value on load step

        result = []
        i = 1
        for job_type in item["job_type"]:
            for job_function in item["job_function"]:
                item_tmp = item.copy()
                item_tmp["job_id_job_type"] = item_tmp["job_id"] + "_" + str(i)
                item_tmp["job_type"] = map_job_type(job_type).lower()
                item_tmp["job_function"] = job_function

                item_tmp["job_hash"] = generate_unique_id(
                    item_tmp["job_id"]
                    + item_tmp["job_name"]
                    + item_tmp["job_function"]
                    + item_tmp["job_type"]
                    + str(item_tmp["posted_date"])
                )

                result.append(item_tmp)

                i = i + 1

        return result
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_job] : {error}")
        return []


def transform_func_voe_job_linkedin(payload, item):
    try:
        item["job_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + str(payload["batch"]["company_id"])
            + "_"
            + item["job_id"]
        )
        item["job_id_job_type"] = item["job_id"]
        item["job_name"] = item["title"]
        item["job_function"] = item["job_function"] or ""
        item["job_type"] = map_job_type(item["job_type"]).lower() or ""
        item["posted_date"] = parse(item["posted_date"])
        item["job_country"] = map_country(item["country"]).lower()
        item["is_classified"] = False

        item[
            "role_seniority"
        ] = ""  # place holder, will be replaced with value on load step
        item["job_hash"] = generate_unique_id(
            item["job_id"]
            + item["job_name"]
            + item["job_function"]
            + item["job_type"]
            + str(item["posted_date"])
        )

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_job] : {error}")
        return []


def transform_func_voe_job_ambitionbox(payload, item):
    try:
        item["job_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + str(payload["batch"]["company_id"])
            + "_"
            + item["JobId"]
        )
        item["job_id_job_type"] = item["job_id"]
        item["job_name"] = item["Title"]
        item["job_type"] = item["EmploymentType"]

        # get country
        locations = item.get("Locations")
        if locations:
            country, code = helpers.search_country_mouthshut(
                locations[0].replace("/", " ")
            )
            item["job_country"] = country
            item["job_country_code"] = code
        else:
            item["job_country"] = None
            item["job_country_code"] = None

        # parse datetime create job
        date_raw = item.get("PostedOn", "")
        date = (
            date_raw.replace("y", " year").replace("d", " day").replace("mon", " month")
        )

        item["posted_date"] = parse(date)

        item["job_function"] = item["FunctionalArea"]
        item["role_seniority"] = ""
        item["is_classified"] = False

        item["job_hash"] = generate_unique_id(
            item["job_id"] + item["job_name"] + item["job_function"]
        )

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_job] : {error}")
        return []


def transform_func_voe_job_csv(payload, item):
    try:
        item["job_id_job_type"] = item["job_id"]
        item["job_name"] = item["title"]
        item["job_country"] = item["country"].lower()
        item["posted_date"] = parse(item["posted_at"])
        item["role_seniority"] = ""
        item["is_classified"] = False

        item["job_hash"] = generate_unique_id(
            item["job_id"]
            + item["job_name"]
            + item["job_function"]
            + str(item["posted_date"])
        )

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_job] : {error}")
        return []


def transform_func_voe_job(payload, item):
    try:
        if payload.get("batch", {}).get("source_type", "").lower() == "csv":
            return transform_func_voe_job_csv(payload, item)
        elif payload["batch"]["source_code"].lower() == "Glassdoor".lower():
            return transform_func_voe_job_glassdoor(payload, item)
        elif payload["batch"]["source_code"].lower() == "Indeed".lower():
            return transform_func_voe_job_indeed(payload, item)
        elif payload["batch"]["source_code"].lower() == "linkedin":
            return transform_func_voe_job_linkedin(payload, item)
        elif payload["batch"]["source_code"].lower() == "ambitionbox":
            return transform_func_voe_job_ambitionbox(payload, item)
        else:
            raise Exception(
                f"Not support source_code={payload['batch']['source_code']}"
            )
    except Exception as error:
        logger.exception(f"[App.transform_func_voe_job] : {error}")
        return []


def transform_func_voe_review_glassdoor(payload, item):
    if "date" not in item:
        raise Exception("date is not exist")
    if "review_id" not in item:
        raise Exception("review_id is not exist")
    if "rating" not in item:
        raise Exception("rating is not exist")

    try:
        results = []

        # item["__review_date__"] = parse(item["date"])
        unique_str = str(payload["batch"]["company_id"])
        item["date"] = parse(item["date"]).isoformat()
        review_id = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["date"]
            + "_"
            + item["review_id"]
        )
        item["review_id"] = review_id
        # default
        item["parent_review_id"] = review_id
        item["technical_type"] = "all"

        item["rating"] = float(item["rating"])
        item["user_name"] = ""

        item["review_hash"] = generate_unique_id(
            item["review_id"]
            + item["pros"].replace("\n", "").replace("<br/>", ".").strip()
            + item["cons"].replace("\n", "").replace("<br/>", ".").strip()
            + item["advice"].replace("\n", "").replace("<br/>", ".").strip()
        )

        # technical review split
        # pros
        if item.get("pros"):
            item_pros = item.copy()
            item_pros["review_id"] = item_pros["parent_review_id"] + "_pros"
            item_pros["review"] = (
                item["pros"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_pros["technical_type"] = "pros"
            item_pros["review_hash"] += f'_{item_pros["technical_type"]}'
            if item_pros["review"]:
                results.append(item_pros)

        # cons
        if item.get("cons"):
            item_cons = item.copy()
            item_cons["review_id"] = item_cons["parent_review_id"] + "_cons"
            item_cons["review"] = (
                item["cons"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_cons["technical_type"] = "cons"
            item_cons["review_hash"] += f'_{item_cons["technical_type"]}'
            if item_cons["review"]:
                results.append(item_cons)

        # recommendation
        if item.get("advice"):
            item_recommendations = item.copy()
            item_recommendations["review_id"] = (
                item_recommendations["parent_review_id"] + "_recommendations"
            )
            item_recommendations["review"] = (
                item["advice"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_recommendations["technical_type"] = "recommendations"
            item_recommendations[
                "review_hash"
            ] += f'_{item_recommendations["technical_type"]}'
            if item_recommendations["review"]:
                results.append(item_recommendations)

        return results

    except Exception as error:
        print(item["date"])
        logger.exception(f"[App.transform] : {error}")
        return results


def transform_func_voe_review_indeed(payload, item):
    if "date" not in item:
        raise Exception("date is not exist")
    if "reviewId" not in item:
        raise Exception("reviewId is not exist")
    if "rating" not in item:
        raise Exception("rating is not exist")

    try:
        results = []
        # item["__review_date__"] = parse(item["date"])
        unique_str = str(payload["batch"]["company_id"])
        item["date"] = parse(item["date"]).isoformat()
        review_id = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["date"]
            + "_"
            + item["reviewId"]
        )
        item["review_id"] = review_id
        # default
        item["parent_review_id"] = review_id
        item["technical_type"] = "all"
        item["rating"] = float(item["rating"])
        item["user_name"] = ""
        item["review_hash"] = generate_unique_id(
            item["review_id"]
            + item["review"].replace("\n", "").replace("<br/>", ".").strip()
            + item["pros"].replace("\n", "").replace("<br/>", ".").strip()
            + item["cons"].replace("\n", "").replace("<br/>", ".").strip()
        )
        # technical review split
        # overall
        if item.get("review"):
            item_overall = item.copy()
            item_overall["review_id"] = item_overall["parent_review_id"] + "_overall"
            item_overall["review"] = (
                item["review"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_overall["technical_type"] = "overall"
            item_overall["review_hash"] += f'_{item_overall["technical_type"]}'
            if item_overall["review"]:
                results.append(item_overall)

        # pros
        if item.get("pros"):
            item_pros = item.copy()
            item_pros["review_id"] = item_pros["parent_review_id"] + "_pros"
            item_pros["review"] = (
                item["pros"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_pros["technical_type"] = "pros"
            item_pros["review_hash"] += f'_{item_pros["technical_type"]}'
            if item_pros["review"]:
                results.append(item_pros)

        # cons
        if item.get("cons"):
            item_cons = item.copy()
            item_cons["review_id"] = item_cons["parent_review_id"] + "_cons"
            item_cons["review"] = (
                item["cons"].replace("\n", "").replace("<br/>", ".").strip()
            )
            item_cons["technical_type"] = "cons"
            item_cons["review_hash"] += f'_{item_cons["technical_type"]}'
            if item_cons["review"]:
                results.append(item_cons)

        return results
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return results


def transform_func_voe_review_ambitionbox(payload, item):
    try:
        results = []
        unique_str = str(payload["batch"]["company_id"])
        item["date"] = parse(item["created_at"].replace("posted on", "")).isoformat()
        item["rating"] = float(item["rating"])
        item["user_name"] = item["user_name"]
        item["review_id"] = (
            payload["batch"]["source_name"]
            + "_"
            + unique_str
            + "_"
            + item["date"]
            + "_"
            + item["review_id"]
        )
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type must be "all" by default
        item["review_hash"] = generate_unique_id(
            item["review_id"]
            + str(item["likes"])
            + str(item["dislikes"])
            + str(item["work_details"])
        )

        # technical review split
        # overall
        if item.get("work_details"):
            item_overall = item.copy()
            item_overall["review_id"] = item_overall["parent_review_id"] + "_overall"
            item_overall["review"] = item["work_details"]
            item_overall["technical_type"] = "overall"
            item_overall["review_hash"] += f'_{item_overall["technical_type"]}'
            if item_overall["review"]:
                results.append(item_overall)

        # pros
        if item.get("likes"):
            item_likes = item.copy()
            item_likes["review_id"] = item_likes["parent_review_id"] + "_pros"
            item_likes["review"] = item["likes"]
            item_likes["technical_type"] = "pros"
            item_likes["review_hash"] += f'_{item_likes["technical_type"]}'
            if item_likes["review"]:
                results.append(item_likes)

        # cons
        if item.get("dislikes"):
            item_dislikes = item.copy()
            item_dislikes["review_id"] = item_dislikes["parent_review_id"] + "_cons"
            item_dislikes["review"] = item["dislikes"]
            item_dislikes["technical_type"] = "cons"
            item_dislikes["review_hash"] += f'_{item_dislikes["technical_type"]}'
            if item_dislikes["review"]:
                results.append(item_dislikes)

        return results

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return results


def transform_func_voe_review_csv(payload, item):
    try:
        item["date"] = parse(item["review_at"]).isoformat()
        item["rating"] = float(item["rating"])
        item["user_name"] = ""
        # default parameters for non-technical reviews
        item["parent_review_id"] = item[
            "review_id"
        ]  # parent review id and review id is the same
        item["technical_type"] = "all"  # technical type is "all"
        item["review_hash"] = generate_unique_id(item["review_id"] + item["review"])

        return [item]
    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_func_voe_review(payload, item):
    try:
        if "batch" not in payload:
            raise Exception("batch is not exist")
        if "source_code" not in payload["batch"]:
            raise Exception("source_code is not exist")

        transformed_items = []
        if payload.get("batch", {}).get("source_type", "").lower() == "csv":
            transformed_items = transform_func_voe_review_csv(payload, item)
        elif payload["batch"]["source_code"].lower() == "Glassdoor".lower():
            transformed_items = transform_func_voe_review_glassdoor(payload, item)
        elif payload["batch"]["source_code"].lower() == "Indeed".lower():
            transformed_items = transform_func_voe_review_indeed(payload, item)
        elif payload["batch"]["source_code"].lower() == "Ambitionbox".lower():
            transformed_items = transform_func_voe_review_ambitionbox(payload, item)
        else:
            raise Exception(
                f"Not support source_code={payload['batch']['source_code']}"
            )

        return transformed_items

    except Exception as error:
        logger.exception(f"[App.transform_func] : {error}")
        return []


def transform_func(payload, item):
    return etl_util_v2.transform_func(
        payload,
        item,
        # web
        transform_func_review_stats=transform_func_review_stats,
        transform_func_voc_review=transform_func_voc,
        transform_func_voe_review=transform_func_voe_review,
        transform_func_voe_job=transform_func_voe_job,
        transform_func_voe_overview=transform_func_voe_overview,
        transform_func_coresignal_stats=transform_func_coresignal_stats,
        transform_func_coresignal_employees=transform_func_coresignal_employees
    )


def __write_dst_file(items, filename):
    with open(filename, "w", newline="") as csvfile:
        if len(items) <= 0:
            return
        fieldnames = items[0].keys()
        writer = csv.DictWriter(
            csvfile, fieldnames=fieldnames, delimiter=DELIMITER, quotechar='"'
        )
        writer.writeheader()
        for item in items:
            writer.writerow(item)


def _load_items(payload, transformed_items):
    output_file = None
    dst_dir = config.DST_DIR
    dst_step_type = "preprocess"
    gcp_bucket_name = config.GCP_STORAGE_BUCKET
    try:
        # Write transformed file to local
        output_file = (
            f"{dst_dir}/{payload['step_detail']['step_detail_name'].strip()}.csv"
        )
        common_util.mkdirs_if_not_exists(dst_dir)
        __write_dst_file(transformed_items, output_file)

        output_filename = f"{payload['step_detail']['step_detail_name'].strip()}.csv"

        # Upload translated file to cloud storage
        gcs_file = f"{dst_step_type}/{payload['batch']['batch_name']}/{output_filename}"
        logger.info(f"Load file to GCS: src={output_file}, dest={gcs_file}")
        gcs_util.upload_google_storage(gcp_bucket_name, output_file, gcs_file)
    except Exception as error:
        logger.exception(f"[Error_Load]: {error}")
        raise error
    finally:
        if output_file:
            os.remove(output_file)


def load_func_review_stats(payload, transformed_items):
    return _load_items(payload, transformed_items)


def load_func_coresignal_stats(payload, transformed_items):
    return _load_items(payload, transformed_items)


def load_func_coresignal_employees(payload, transformed_items):
    return _load_items(payload, transformed_items)


def load_func_voc_review(payload, transformed_items):
    data_version = payload["batch"]["data_version"]
    if data_version == 1:
        return _load_items(payload, transformed_items)

    hash_key = "review_hash"
    items_hash_list = [item[hash_key] for item in transformed_items]

    existed_items = db[config.MONGODB_VOC_REVIEW_COLLECTION].find(
        filter={
            "company_datasource_id": payload["batch"]["company_datasource_id"],
            hash_key: {"$in": items_hash_list},
        },
        projection=[hash_key],
    )

    existed_review_hashed = [item.get(hash_key) for item in existed_items]
    new_transformed_items = [
        item
        for item in transformed_items
        if item.get(hash_key) not in existed_review_hashed
    ]
    existed_items.close()
    return _load_items(payload, new_transformed_items)


def load_func_voe_review(payload, transformed_items):
    data_version = payload["batch"]["data_version"]
    if data_version == 1:
        return _load_items(payload, transformed_items)

    hash_key = "review_hash"
    items_hash_list = [item[hash_key] for item in transformed_items]

    existed_items = db[config.MONGODB_VOE_REVIEW_COLLECTION].find(
        filter={
            "company_datasource_id": payload["batch"]["company_datasource_id"],
            hash_key: {"$in": items_hash_list},
        },
        projection=[hash_key],
    )

    existed_review_hashed = [item.get(hash_key) for item in existed_items]
    new_transformed_items = [
        item
        for item in transformed_items
        if item.get(hash_key) not in existed_review_hashed
    ]
    existed_items.close()

    return _load_items(payload, new_transformed_items)


def load_func_voe_overview(payload, transformed_items):
    data_version = payload["batch"]["data_version"]
    if data_version == 1:
        return _load_items(payload, transformed_items)

    hash_key = "overview_hash"
    items_hash_list = [item[hash_key] for item in transformed_items]

    existed_items = db[config.MONGODB_VOE_OVERVIEW_COLLECTION].find(
        filter={
            "company_datasource_id": payload["batch"]["company_datasource_id"],
            hash_key: {"$in": items_hash_list},
        },
        projection=[hash_key],
    )

    existed_review_hashed = [item.get(hash_key) for item in existed_items]
    new_transformed_items = [
        item
        for item in transformed_items
        if item.get(hash_key) not in existed_review_hashed
    ]
    existed_items.close()

    return _load_items(payload, new_transformed_items)


def load_func_voe_job(payload, transformed_items):
    data_version = payload["batch"]["data_version"]
    if data_version == 1:
        return _load_items(payload, transformed_items)

    hash_key = "job_hash"
    items_hash_list = [item[hash_key] for item in transformed_items]

    existed_items = db[config.MONGODB_VOE_OVERVIEW_COLLECTION].find(
        filter={
            "company_datasource_id": payload["batch"]["company_datasource_id"],
            hash_key: {"$in": items_hash_list},
        },
        projection=[hash_key],
    )

    existed_review_hashed = [item.get(hash_key) for item in existed_items]
    new_transformed_items = [
        item
        for item in transformed_items
        if item.get(hash_key) not in existed_review_hashed
    ]
    existed_items.close()

    return _load_items(payload, new_transformed_items)


def load_func_voc(payload, transformed_items):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW.value.lower()
    ):
        return load_func_voc_review(payload, transformed_items)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
    ):
        return load_func_review_stats(payload, transformed_items)
    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )


def load_func_voe(payload, transformed_items):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW.value.lower()
    ):
        return load_func_voe_review(payload, transformed_items)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
    ):
        return load_func_review_stats(payload, transformed_items)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.JOB.value.lower()
    ):
        return load_func_voe_job(payload, transformed_items)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.OVERVIEW.value.lower()
    ):
        return load_func_voe_overview(payload, transformed_items)
    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )
        
        
def load_func_coresignal(payload, transformed_items):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower()
    ):
        return load_func_coresignal_stats(payload, transformed_items)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
    ):
        return load_func_coresignal_employees(payload, transformed_items)

    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )


def load_func(payload, transformed_items):
    if "step_detail" not in payload:
        raise Exception("step_detail is not exist")

    if payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOC.value.lower():
        return load_func_voc(payload, transformed_items)
    elif (
        payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOE.value.lower()
    ):
        return load_func_voe(payload, transformed_items)
    elif (
        payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.HR.value.lower()
    ):
        return load_func_coresignal(payload, transformed_items)
    else:
        raise Exception(
            f"Not support nlp_type: {payload['step_detail']['meta_data']['nlp_type']}"
        )


def handle_task(payload):
    # logger.info(payload)
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
        src_step_type="crawl",
        dst_step_type="preprocess",
        src_dir=config.SRC_DIR,
        dst_dir=config.DST_DIR,
        transform_func=transform_func,
        load_func=load_func,
        progress_threshold=config.PROGRESS_THRESHOLD,
        transform_batch_size=config.TRANSFORM_BATCH_SIZE,
        thread_enable=1,
        thread_count=10,
    )
    # Begin consume task

    # Init mongodb
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)

    pubsub_util.subscribe(
        logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task
    )
