import pymongo
import sys

sys.path.append("../")

import config


def create_mongodb_client():
    client = pymongo.MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    return db


def _get_review_documents_for_company_datasource(
    db,
    collection,
    company_datasource_id,
    to_version,
    nlp_pack,
    from_version=0,
    load_eng_only=False,
):
    """
    Get reviews for a given company datasource as a cursor that can be iterated over.
    This is calculated from the side of MongoDB using aggregation pipeline
    """
    # $match stage
    match_stage = {
        "$match": {
            "company_datasource_id": company_datasource_id,
            "data_version": {"$gt": from_version, "$lte": to_version},
            f"nlp_results.{nlp_pack}": {
                "$exists": True
            },  # only documents that has this nlp_pack
        }
    }
    # if instructed, only load review with original English language
    if load_eng_only:
        match_stage["$match"]["review_language"] = "en"

    # $unwind (unnest) stage
    unwind_stage = {"$unwind": f"$nlp_results.{nlp_pack}"}

    # $project stage
    common_projection_keys = (
        "created_at",
        "review_id",
        "source_name",
        "company_name",
        "nlp_pack",
        "nlp_type",
        "user_name",
        "review",
        "trans_review",
        "trans_status",
        "rating",
        "batch_id",
        "batch_name",
        "file_name",
        "review_date",
        "company_id",
        "source_id",
        "step_id",
        "request_id",
        "case_study_id",
        "parent_review_id",
        "technical_type",  # all standards ones
        "company_datasource_id"
    )
    # construct projection dict with common keys first
    project_stage = {"$project": {key: 1 for key in common_projection_keys}}
    # add nlp result fields
    project_stage["$project"].update(
        {
            "review_country": {"$ifNull": ["$review_country", "Unknown"]},
            "country_code": {"$ifNull": ["$review_country_code", "Unknown"]},
            "language": "$review_language",
            "single_nlp_result": f"$nlp_results.{nlp_pack}",
            "_id": 0,
        }
    )

    # $count stage
    count_stage = {"$count": "num_documents"}

    # construct pipelines
    count_pipeline = [match_stage, unwind_stage, count_stage]
    query_pipeline = [match_stage, unwind_stage, project_stage]

    # make query
    cursor_object = db[collection].aggregate_raw_batches(
        query_pipeline, batchSize=config.BATCH_SIZE
    )

    return cursor_object


def get_voc_reviews_for_company_datasource(
    db,
    company_datasource_id,
    to_version,
    nlp_pack,
    from_version=0,
    load_eng_only=False,
):
    """
    Get VOC review document
    """
    collection = config.MONGODB_VOC_REVIEW_COLLECTION
    return _get_review_documents_for_company_datasource(
        db,
        collection,
        company_datasource_id,
        to_version,
        nlp_pack,
        from_version,
        load_eng_only,
    )


def get_voe_reviews_for_company_datasource(
    db,
    company_datasource_id,
    to_version,
    nlp_pack,
    from_version=0,
    load_eng_only=False,
):
    """
    Get VOE review document
    """
    collection = config.MONGODB_VOE_REVIEW_COLLECTION
    return _get_review_documents_for_company_datasource(
        db,
        collection,
        company_datasource_id,
        to_version,
        nlp_pack,
        from_version,
        load_eng_only,
    )


def get_voe_jobs_for_company_datasource(
    db,
    company_datasource_id,
    to_version_job,
    from_version_job=0,
):
    """
    Get VOE job document
    """
    collection = config.MONGODB_VOE_JOB_COLLECTION
    # filter query
    # if action == sync, from_version is 0. If action == update, from_version is the previous version
    # to_version: to update to
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "data_version": {
            "$gt": from_version_job,
            "$lte": to_version_job,
        },
    }

    # projection
    common_projection_keys = (
        "created_at",
        "case_study_id",
        "source_name",
        "source_id",
        "company_name",
        "company_id",
        "job_id",
        "job_name",
        "job_function",
        "job_type",
        "posted_date",
        "job_country",
        "role_seniority",
        "batch_id",
        "batch_name",
        "file_name",
        "request_id",
        "step_id",
    )
    # construct projection dict with common keys first
    projection_dict = {key: 1 for key in common_projection_keys}
    projection_dict.update({"_id": 0})

    # make query
    cursor_object = db[collection].find_raw_batches(
        filter=filter_dict, projection=projection_dict, batch_size=config.BATCH_SIZE
    )

    return cursor_object


def get_voe_overview_for_company_datasource(
    db,
    company_datasource_id,
    data_version_job,
):
    """
    Get VOE overview document
    Only need to get the given version in anyway.
    """
    collection = config.MONGODB_VOE_OVERVIEW_COLLECTION
    # filter query
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "data_version": {"$lte": data_version_job},  # get all older versions as well
    }

    # projection
    common_projection_keys = (
        "created_at",
        "case_study_id",
        "source_name",
        "source_id",
        "company_name",
        "company_id",
        "min_fte",
        "max_fte",
        "batch_id",
    )
    # construct projection dict with common keys first
    projection_dict = {key: 1 for key in common_projection_keys}
    projection_dict.update({"_id": 0})
    # make query
    cursor_object = db[collection].find(
        filter=filter_dict,
        projection=projection_dict,
    )

    return cursor_object


def _get_review_stats_for_company_datasource(
    db, collection, company_datasource_id, data_version_review_stats
):
    """
    Get review_stats from MongoDB
    """
    # filter query
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "data_version": data_version_review_stats,
    }

    # projection
    common_projection_keys = (
        "created_at",
        "case_study_id",
        "company_name",
        "company_id",
        "source_name",
        "source_id",
        "batch_id",
        "total_reviews",
        "total_ratings",
        "average_rating",
        "company_datasource_id"
    )
    # construct projection dict with common keys first
    projection_dict = {key: 1 for key in common_projection_keys}
    projection_dict.update({"_id": 0})
    # make query
    cursor_object = db[collection].find(
        filter=filter_dict,
        projection=projection_dict,
    )

    return cursor_object


def get_voc_review_stats_for_company_datasource(
    db,
    company_datasource_id,
    data_version_review_stats,
):
    """
    Get VOC review_stats document
    """
    collection = config.MONGODB_VOC_REVIEW_STATS_COLLECTION
    return _get_review_stats_for_company_datasource(
        db, collection, company_datasource_id, data_version_review_stats
    )


def get_voe_review_stats_for_company_datasource(
    db,
    company_datasource_id,
    data_version_review_stats,
):
    """
    Get VOE review_stats document
    """
    collection = config.MONGODB_VOE_REVIEW_STATS_COLLECTION
    return _get_review_stats_for_company_datasource(
        db, collection, company_datasource_id, data_version_review_stats
    )
