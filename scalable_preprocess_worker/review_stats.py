import datetime

from core import logger
import config

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def transform_func_review_stats(payload, item):
    try:
        func_mapping = {
            "applestore": transform_apple_store,
            "ambitionbox": transform_ambitionbox,
            "googleplay": transform_google_playstore,
            "capterra": transform_capterra,
            "gartner": transform_gartner,
            "glassdoor": transform_glassdoor,
            "indeed": transform_indeed,
            "mouthshut": transform_mouthshut,
            "trustpilot": transform_trustpilot,
            "reddit": transform_reddit,
            "g2": transform_g2,
        }

        source_type = payload.get("batch", {}).get("source_type", "").lower()
        source_code = payload.get("batch", {}).get("source_code", "").lower()
        if source_type == "csv":
            transform_func = transform_csv
        else:
            transform_func = func_mapping[source_code.lower()]

        return transform_func(payload, item)

    except Exception as error:
        logger.exception(f"[App.transform_func_review_stats] : {error}")
        return []


def transform_csv(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_ratings"] = (
            int(item["total_ratings"]) if "total_ratings" in item else None
        )
        item["total_reviews"] = (
            int(item["total_reviews"]) if "total_reviews" in item else None
        )
        item["average_rating"] = (
            int(item["average_rating"]) if "average_rating" in item else None
        )

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_apple_store(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_ratings"] = (
            int(float(item["total_ratings"])) if "total_ratings" in item else None
        )
        item["average_rating"] = (
            float(item["average_rating"]) if "average_rating" in item else None
        )
        logger.info(f"transform app store preprocess : {str([item])}")

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_ambitionbox(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        # transform total reviews
        num_reviews = None
        if "total_reviews" in item:
            num_reviews = item["total_reviews"]
            identifiers = {"k": 1e3, "m": 1e6, "b": 1e9}
            for k, v in identifiers.items():
                if k in num_reviews:
                    num_reviews = num_reviews.replace(k, "").strip()
                    num_reviews = int(float(num_reviews) * v)
                    break

        item["total_reviews"] = num_reviews
        item["average_rating"] = (
            float(item["average_rating"]) if "average_rating" in item else None
        )

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_google_playstore(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_ratings"] = int(float(item["ratings"]))
        item["average_rating"] = float(item["score"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_capterra(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["reviews_total"]))
        item["average_rating"] = float(item["overall_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_gartner(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_reviews"]))
        item["total_ratings"] = int(float(item["total_ratings"]))
        item["average_rating"] = float(item["average_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_glassdoor(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_reviews"]))
        item["average_rating"] = float(item["average_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_indeed(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_reviews"]))
        item["average_rating"] = float(item["average_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_mouthshut(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_ratings"] = int(float(item["total_ratings"]))
        item["average_rating"] = float(item["average_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_reddit(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_submissions"]))
        item["average_rating"] = None

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_trustpilot(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_reviews"]))
        item["average_rating"] = float(item["trust_score"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []


def transform_g2(payload, item):
    try:
        item["__review_stats_date__"] = datetime.datetime.now().date()
        item["total_reviews"] = int(float(item["total_reviews"]))
        item["average_rating"] = float(item["average_rating"])

        return [item]

    except Exception as error:
        logger.exception(f"[App.transform] : {error}")
        return []
