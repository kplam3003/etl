import pymongo
import asyncio
import time
import sys
sys.path.append('../')

from pymongo.collection import ReturnDocument

import config


def create_mongodb_client():
    client = pymongo.MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    return db


def _get_review_document_for_company_datasource(
    db,
    collection,
    company_datasource_id, 
    data_version, 
    re_nlp, 
    nlp_pack, 
    get_older_versions=True
):
    """
    Get reviews for a given company datasource as a cursor that can be iterated over.
    """
    projection = [
        "company_datasource_id", "review_id", "review_hash", "data_version", "trans_review", "nlp_results", "trans_status"
    ]
    
    # construct filter
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "data_version": {"$lte": data_version} # get all older versions as well
    }
    # not re_nlp => only query reviews without this nlp_pack
    if not re_nlp:
        filter_dict.update({
            "nlp_results.__NLP_PACKS__": {"$ne": nlp_pack}
        })
    # if specified, will only git this specific version
    if not get_older_versions:
        filter_dict.update({
            "data_version": data_version
        })

    # make query
    num_documents = db[collection].count_documents(filter=filter_dict)
    cursor_object = db[collection].find(
        filter=filter_dict,
        projection=projection
    )
    
    return cursor_object, num_documents


def get_voc_review_for_company_datasource(
    db, 
    company_datasource_id, 
    data_version, 
    re_nlp, 
    nlp_pack, 
    get_older_versions=True
):
    """
    Get VOC review document
    """
    collection = config.MONGODB_VOC_REVIEW_COLLECTION
    return _get_review_document_for_company_datasource(
        db, collection, company_datasource_id, data_version, re_nlp, nlp_pack, get_older_versions
    )
    
    
def get_voe_review_for_company_datasource(
    db, 
    company_datasource_id, 
    data_version, 
    re_nlp, 
    nlp_pack, 
    get_older_versions=True
):
    """
    Get VOE review document
    """
    collection = config.MONGODB_VOE_REVIEW_COLLECTION
    return _get_review_document_for_company_datasource(
        db, collection, company_datasource_id, data_version, re_nlp, nlp_pack, get_older_versions
    )
    

def get_voe_job_for_company_datasource(
    db, 
    company_datasource_id, 
    data_version, 
    re_classify=False, 
    get_older_versions=True
):
    """
    Get VOE review document
    """
    collection = config.MONGODB_VOE_JOB_COLLECTION
    projection = [
        "company_datasource_id", "job_id", "job_hash", "job_name", "job_function", "is_classified"
    ]
    
    # construct filter
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "data_version": {"$lte": data_version} # get all older versions as well
    }
    # not re_classify => only query reviews without this nlp_pack
    if not re_classify:
        filter_dict.update({
            "is_classified": False
        })
    # if specified, will only git this specific version
    if not get_older_versions:
        filter_dict.update({
            "data_version": data_version
        })

    # make query
    num_documents = db[collection].count_documents(filter=filter_dict)
    cursor_object = db[collection].find(
        filter=filter_dict,
        projection=projection
    )
    
    return cursor_object, num_documents


def _update_review_nlp_result(
    db, 
    collection,
    company_datasource_id, 
    review_hash, 
    nlp_pack, 
    nlp_results
):
    """
    Update a review document with the nlp pack and nlp results
    """
    projection = [
        "company_datasource_id", "review_id", "review_hash", "data_version", "trans_review", "nlp_results"
    ]
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "review_hash": review_hash
    }
    updated_fields = {
        "$set": {f"nlp_results.{nlp_pack}": nlp_results},
        "$addToSet": {"nlp_results.__NLP_PACKS__": nlp_pack}
    }
    updated_document = db[collection].find_one_and_update(
        filter=filter_dict,
        projection=projection,
        update=updated_fields,
        return_document=ReturnDocument.AFTER
    )

    return updated_document
    
    
def update_voc_review_nlp_result(
    db, 
    company_datasource_id, 
    review_hash, 
    nlp_pack, 
    nlp_results
):
    """
    Update a VOC review document with the nlp pack and nlp results
    """
    return _update_review_nlp_result(
        db, config.MONGODB_VOC_REVIEW_COLLECTION, company_datasource_id, review_hash, nlp_pack, nlp_results
    )
    
    
def update_voe_review_nlp_result(
    db, 
    company_datasource_id, 
    review_hash, 
    nlp_pack, 
    nlp_results
):
    """
    Update a VOE review document with the nlp pack and nlp results
    """
    return _update_review_nlp_result(
        db, config.MONGODB_VOE_REVIEW_COLLECTION, company_datasource_id, review_hash, nlp_pack, nlp_results
    )

    
def update_voe_job_function(db, company_datasource_id, job_hash, predicted_job_function):
    """
    Update predicted job function into job_function field, and set is_classified to True
    """
    filter_dict = {
        "company_datasource_id": company_datasource_id,
        "job_hash": job_hash
    }
    updated_fields = {
        "$set": {
            "job_function": predicted_job_function,
            "is_classified": True
        },
    }
    updated_document = db[config.MONGODB_VOE_JOB_COLLECTION].find_one_and_update(
        filter=filter_dict,
        update=updated_fields,
        return_document=ReturnDocument.AFTER
    )
    
    return updated_document