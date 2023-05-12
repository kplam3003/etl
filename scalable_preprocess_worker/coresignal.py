import datetime
import json

from dateparser import parse

from core import logger
from helpers import parse_date_from_string
from coresignal_helpers import find_previous_next_company, check_starter_leaver, encode_job_title
from google_places import parse_location
import config

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
CORESIGNAL_LINKEDIN_MEMBER_KEYS = [
    "member_also_viewed_collection",
    "member_awards_collection",
    "member_certifications_collection",
    "member_courses_collection",
    "member_courses_suggestion_collection",
    "member_education_collection",
    "member_experience_collection",
    "member_groups_collection",
    "member_interests_collection",
    "member_languages_collection",
    "member_organizations_collection",
    "member_patents_collection",
    "member_posts_see_more_urls_collection",
    "member_projects_collection",
    "member_publications_collection",
    "member_recommendations_collection",
    "member_similar_profiles_collection",
    "member_skills_collection",
    "member_test_scores_collection",
    "member_volunteering_cares_collection",
    "member_volunteering_opportunities_collection",
    "member_volunteering_positions_collection",
    "member_volunteering_supports_collection",
    "member_websites_collection",
]


# TRANSFORM
# coresignal_stats
def transform_func_coresignal_stats(payload, item):
    try:
        func_mapping = {
            "coresignal linkedin": transform_func_coresignal_stats_linkedin,
        }

        source_code = payload.get("batch", {}).get("source_code", "").lower()
        transform_func = func_mapping[source_code.lower()]

        return transform_func(payload, item)

    except Exception as error:
        logger.exception(f"[App.transform_func_coresignal_stats] : {error}")
        return []


def transform_func_coresignal_stats_linkedin(payload, item):
    # extract data
    data = json.loads(item["data"])
    item["coresignal_name"] = data["name"]
    item["coresignal_shorthand_name"] = data["company_shorthand_name"]
    item["coresignal_id"] = data["id"]
    item["coresignal_created"] = data["created"]
    item["coresignal_last_updated"] = data["last_updated"]
    item["coresignal_deleted"] = data["deleted"]
    item["coresignal_last_updated_ux"] = parse(str(data["last_updated_ux"]))
    item["employees_count"] = data["employees_count"]

    item["__profile_date__"] = datetime.datetime.utcnow().date().isoformat()

    return [item]


# coresignal_employees
def transform_func_coresignal_employees(payload, item):
    try:
        func_mapping = {
            "coresignal linkedin": transform_func_coresignal_employees_linkedin,
        }

        source_code = payload.get("batch", {}).get("source_code", "").lower()
        transform_func = func_mapping[source_code.lower()]

        return transform_func(payload, item)

    except Exception as error:
        logger.exception(f"[App.transform_func_coresignal_employees] : {error}")
        return []


def transform_func_coresignal_employees_linkedin(payload, item):
    # extract metadata from payload
    coresignal_company_ids = payload["step_detail"]["meta_data"]["coresignal_company_ids"]
    leo_company_id = payload["batch"]["company_id"]
    # extract data
    data = json.loads(item["data"])
    item = data.copy()

    # check if item existed
    existed = item.get("existed", False)
    
    # process top-level info
    # only process some info if not existed
    if not existed:
        item["__profile_date__"] = datetime.datetime.utcnow().date().isoformat()       
        item["coresignal_name"] = data["name"]
        item["coresignal_id"] = data["id"]
        item["coresignal_created"] = data["created"]
        item["coresignal_last_updated"] = data["last_updated"]
        item["coresignal_deleted"] = data["deleted"]
        item["coresignal_last_updated_ux"] = parse(str(data["last_updated_ux"])).isoformat()

    # parse location
    location_results = parse_location(data["location"], logger=logger)
    item.update(location_results)

    # process each member
    # NOTE: these transformations will also be applied to existing items
    # This is to make sure that when new rules are added, or old rules updated
    # it is reflected in the items that has already been persisted in MongoDB 
    for collection_key in CORESIGNAL_LINKEDIN_MEMBER_KEYS:
        collection_items = item[collection_key]
        # first filter out deleted records
        not_deleted_items = filter(lambda d: d.get("deleted", 0) == 0, collection_items)
        processed_items = []
        for collection_item in not_deleted_items:
            # parse date_from and date_to
            if "date_from" in collection_item.keys():
                collection_item["date_from"] = parse_date_from_string(
                    date_string=collection_item["date_from"],
                    first_day=True,
                    date_only=True,
                )
            if "date_to" in collection_item.keys():
                collection_item["date_to"] = parse_date_from_string(
                    date_string=collection_item["date_to"],
                    first_day=False,
                    date_only=True,
                )
            # parse location
            if "location" in collection_item.keys():
                search_location = collection_item["location"]
                if search_location:
                    _location_results = parse_location(
                        search_location=search_location, logger=logger
                    )
                    collection_item.update(_location_results)

            processed_items.append(collection_item)

        # COLLECTION-SPECIFIC PROCESSING
        # NOTE: can be refactored to individual functions if becomes unwieldy
        ## Experience
        if "experience" in collection_key:
            # LEON-124, LEON-125 previous/next company processing
            processed_items = find_previous_next_company(
                member_experiences=processed_items,
                leo_company_id=leo_company_id,
                ref_coresignal_company_ids=coresignal_company_ids,
            )
            processed_items = check_starter_leaver(
                member_experiences=processed_items,
                leo_company_id=leo_company_id,
                ref_coresignal_company_ids=coresignal_company_ids
            )
            
            # LEON-126 embed titles
            title_embeddings = encode_job_title(processed_items)
            for exp in processed_items:
                _id = exp["id"]
                exp["title_embeddings"] = title_embeddings.get(_id, None)

        # replace with processed version
        item[collection_key] = processed_items

    return_item = {"data": json.dumps(item, default=str)}
    return [return_item]
