import requests
import time
import json
from datetime import datetime
import sys
sys.path.append('../')

import meaningcloud
import concurrent.futures
from urllib import parse

import config
import helpers
import mongodb_helpers
from core import pubsub_util
from core.logger import init_logger, auto_logger

logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def process_single_voc_review(doc, nlp_type, nlp_pack, db):
    """
    Process a single voc review document
    """
    start_time = datetime.utcnow()
    try:
        company_datasource_id = doc['company_datasource_id']
        review_hash = doc['review_hash']
        # do nlp
        start_nlp_time = time.time()
        if config.USE_TPP_NLP_SERVICE:
            nlp_results = helpers.do_local_nlp_on_review_doc(
                doc=doc, 
                nlp_type=nlp_type, 
                nlp_pack=nlp_pack, 
                logger=logger
            )
        else:
            nlp_results = helpers.do_MC_nlp_on_review_doc(
                doc=doc, 
                nlp_type=nlp_type, 
                nlp_pack=nlp_pack, 
                logger=logger
            )
            
        end_nlp_time = time.time()
        # update 
        updated_document = mongodb_helpers.update_voc_review_nlp_result(
            db=db,
            company_datasource_id=company_datasource_id,
            review_hash=review_hash,
            nlp_pack=nlp_pack,
            nlp_results=nlp_results
        )
        end_update_time = time.time()
        # only return true if document is properly updated
        if updated_document: # document actually exists
            nlp_results = updated_document.get('nlp_results', {})
            if nlp_pack in nlp_results: # nlp_pack is a key of nlp_results field (list)
                logger.info(
                    f"[process_single_voc_review] SUCCESS - "
                    f"text: {updated_document['trans_review']} - "
                    f"nlp_results: {nlp_results} - "
                    f"took: {datetime.utcnow() - start_time} - "
                    f"NLP time: {end_nlp_time - start_nlp_time:.4f} - "
                    f"MongoDB update time: {end_update_time - end_nlp_time:.4f} - "
                )
                return True
        
        # return false otherwise
        logger.warning(f"[process_single_voc_review] MONGODB UPDATE FAILED - text: {updated_document['trans_review']} - "
                    f"nlp_results: {nlp_results}")
        return False
    
    except:
        logger.exception(f"[process_single_voe_review] error happened while processing "
                         f"review {doc['review_id']}")
        return False
    

def process_single_voe_review(doc, nlp_type, nlp_pack, db):
    """
    Process a single voe review document
    """
    try:
        company_datasource_id = doc['company_datasource_id']
        review_hash = doc['review_hash']
        # do nlp
        nlp_results = helpers.do_MC_nlp_on_review_doc(doc=doc, nlp_type=nlp_type, nlp_pack=nlp_pack, logger=logger)
        # update 
        updated_document = mongodb_helpers.update_voe_review_nlp_result(
            db=db,
            company_datasource_id=company_datasource_id,
            review_hash=review_hash,
            nlp_pack=nlp_pack,
            nlp_results=nlp_results
        )
        
        # only return true if document is properly updated
        if updated_document: # document actually exists
            nlp_results = updated_document.get('nlp_results', {})
            if nlp_pack in nlp_results: # nlp_pack is a key of nlp_results field (list)
                logger.info(f"[process_single_voe_review] SUCCESS - text: {updated_document['trans_review']} - "
                            f"nlp_results: {nlp_results}")
                return True
        
        # return false otherwise
        logger.warning(f"[process_single_voe_review] MONGODB UPDATE FAILED - text: {updated_document['trans_review']} - "
                    f"nlp_results: {nlp_results}")
        return False
    
    except:
        logger.exception(f"[process_single_voe_review] error happened while processing "
                         f"review {doc['review_id']}")
        return False

    
def process_single_voe_job(doc, db):
    """
    Process a single VOE Job document
    """
    try:
        company_datasource_id = doc['company_datasource_id']
        job_hash = doc['job_hash']
        predicted_job_function = helpers.classify_job_name_from_doc(doc, logger=logger)
        mongodb_helpers.update_voe_job_function(
            db=db, 
            company_datasource_id=company_datasource_id, 
            job_hash=job_hash,
            predicted_job_function=predicted_job_function
        )
        return True
    
    except:
        logger.exception(f"[process_single_voe_job] error happened while processing "
                         f"job {doc['job_id']}")
        return False


def process_voc_reviews_for_company_datasource(
    company_datasource_id, 
    data_version, 
    nlp_type, 
    nlp_pack, 
    re_nlp, 
    logger
):
    """
    process reviews from VOC company datasource
    - Get reviews based on re_nlp
    - send reviews through to Meaning Cloud for NLP
    - update results into MongoDB review document.
    
    The sending reviews and updating document is handed by multithreading executor
    """
    logger.info(f"[process_voc_reviews_company_datasource] "
                f"start doing NLP for company_datasource_id {company_datasource_id} - "
                f"data_version {data_version} - nlp_pack {nlp_pack} - re_nlp {re_nlp}")
    # initiate multithreading executor
    start_time = datetime.utcnow()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.WORKER_THREAD_COUNT)
    
    # get reviews
    db = mongodb_helpers.create_mongodb_client()
    cursor, num_documents = mongodb_helpers.get_voc_review_for_company_datasource(
        db=db,
        company_datasource_id=company_datasource_id,
        data_version=data_version,
        re_nlp=re_nlp,
        nlp_pack=nlp_pack,
        get_older_versions=True
    )
    
    if num_documents == 0:
        logger.warning(f"[process_voc_reviews_company_datasource] 0 documents found!")
        return []
    
    # start doing nlp
    result_statuses = []
    futures = []
    # pull documents from cursor and send to threads to process
    for doc in cursor:
        future = executor.submit(
            fn=process_single_voc_review, 
            doc=doc,
            nlp_type=nlp_type,
            nlp_pack=nlp_pack,
            db=db,
        )
        futures.append(future)
        
    # iterate over results when their tasks are completed
    i = 0
    for future in concurrent.futures.as_completed(futures):
        result_statuses.append(future.result())
        i += 1
        if (i % config.PROGRESS_THRESHOLD == 0) or (i == num_documents):
            progress = i / num_documents
            logger.info(f"[process_voc_reviews_company_datasource] "
                        f"progress {progress:.4f}, {i}/{num_documents}")
    
    success_rate = sum(result_statuses) / len(result_statuses)
    cursor.close()
    logger.info(
        f"[process_voc_reviews_company_datasource] finished parsing, "
        f"success rate {success_rate:.4f}, "
        f"took: {datetime.utcnow() - start_time}"
    )
    return result_statuses
    
    
def process_voe_reviews_for_company_datasource(
    company_datasource_id, 
    data_version, 
    nlp_type, 
    nlp_pack, 
    re_nlp, 
    logger
):
    """
    process reviews from VOE company datasource
    - Get reviews based on re_nlp
        - send reviews through to Meaning Cloud for NLP
        - update results into MongoDB review document.
    
    The sending data to prediction service and updating document is handed by multithreading executor
    """
    logger.info(f"[process_voe_reviews_company_datasource] "
                f"start doing NLP for company_datasource_id {company_datasource_id} - "
                f"data_version {data_version} - nlp_pack {nlp_pack} - re_nlp {re_nlp}")
    # initiate multithreading executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.WORKER_THREAD_COUNT)
    
    # get reviews
    db = mongodb_helpers.create_mongodb_client()
    review_cursor, num_documents = mongodb_helpers.get_voe_review_for_company_datasource(
        db=db,
        company_datasource_id=company_datasource_id,
        data_version=data_version,
        re_nlp=re_nlp,
        nlp_pack=nlp_pack,
        get_older_versions=True
    )
    if num_documents == 0:
        logger.warning(f"[process_voe_reviews_company_datasource] 0 documents found!")
        return []
    
    # start doing nlp
    result_statuses = []
    futures = []
    # pull documents from cursor and send to threads to process
    for doc in review_cursor:
        future = executor.submit(
            fn=process_single_voe_review, 
            doc=doc,
            nlp_type=nlp_type,
            nlp_pack=nlp_pack,
            db=db,
        )
        futures.append(future)
        
    # iterate over results when their tasks are completed
    i = 0
    for future in concurrent.futures.as_completed(futures):
        result_statuses.append(future.result())
        i += 1
        if (i % config.PROGRESS_THRESHOLD == 0) or (i == num_documents):
            progress = i / num_documents
            logger.info(f"[process_voe_reviews_company_datasource] "
                        f"progress {progress:.4f}, {i}/{num_documents}")
    
    success_rate = sum(result_statuses) / len(result_statuses)
    review_cursor.close()
    logger.info(f"[process_voe_reviews_company_datasource] finished parsing, success rate {success_rate:.4f}")
    return result_statuses
    
    
def process_voe_jobs_for_company_datasource(
    company_datasource_id, 
    data_version, 
    logger,
    re_classify=False,
):
    """
    process job from VOE company datasource
    - Get not-classified jobs
        - send job name to prediction service for job classification
        - update results into MongoDB job document
    
    The sending data to prediction service and updating document is handled by multithreading executor
    """
    logger.info(f"[process_voe_jobs_company_datasource] "
                f"start classifying jobs for company_datasource_id {company_datasource_id}")
    # initiate multithreading executor
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.WORKER_THREAD_COUNT)
    
    # get reviews
    db = mongodb_helpers.create_mongodb_client()
    review_cursor, num_documents = mongodb_helpers.get_voe_job_for_company_datasource(
        db=db,
        company_datasource_id=company_datasource_id,
        data_version=data_version,
        re_classify=re_classify,
        get_older_versions=True
    )
    if num_documents == 0:
        logger.warning(f"[process_voe_jobs_company_datasource] 0 documents found!")
        return []
    
    # start doing nlp
    result_statuses = []
    futures = []
    # pull documents from cursor and send to threads to process
    for doc in review_cursor:
        future = executor.submit(
            fn=process_single_voe_job, 
            doc=doc,
            db=db
        )
        futures.append(future)
        
    # iterate over results when their tasks are completed
    i = 0
    for future in concurrent.futures.as_completed(futures):
        result_statuses.append(future.result())
        i += 1
        if (i % config.PROGRESS_THRESHOLD == 0) or (i == num_documents):
            _progress = i / num_documents
            scaled_progress = 0.6 + _progress * (1 - 0.6) # scale range [0.0, 1.0] to range [0.6, 1.0]
            logger.info(f"[process_voe_jobs_company_datasource] "
                        f"progress {scaled_progress:.4f}, {i}/{num_documents}")
    
    success_rate = sum(result_statuses) / len(result_statuses)
    review_cursor.close()
    logger.info(f"[process_voe_jobs_company_datasource] finished parsing, success rate {success_rate:.4f}")
    return result_statuses


def handle_task(payload):
    """
    main event loop:
    - if a payload is received, parse payload
    """   
    # parse payload
    request_id = payload["request_id"]
    case_study_id = payload["case_study_id"]
    case_study_payload = payload["payload"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    postprocess_type = payload["postprocess"]["postprocess_type"]
    postprocess_object = payload["postprocess"]
    
    # skip these altogether when nlp_type is HRA since this step is entirely unnecessary
    nlp_type: str = case_study_payload["dimension_config"]["nlp_type"]
    if nlp_type.lower() == "hra":
        aftertask_payload = {
            "type": postprocess_type,
            "event": "finish",
            "case_study_id": case_study_id,
            "request_id": request_id,
            "postprocess_id": postprocess_id,
            "postprocess": postprocess_object,
            "error": "",
        }
        pubsub_util.publish(
            gcp_product_id=config.GCP_PROJECT_ID,
            topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
            payload=aftertask_payload,
            logger=logger,
        )
        return
    
    # get data version changes of each company_datasource to query proper data from mongodb 
    data_version_changes = payload["postprocess"]["meta_data"]["case_study_data_version_changes"]
    data_version_changes_dict = {}
    for dvc in data_version_changes:
        _id = dvc["company_datasource_id"]
        temp_dict = {}
        if dvc["data_type"] == "review":
            temp_dict["previous_data_version"] = dvc["previous_data_version"]
            temp_dict["new_data_version"] = dvc["new_data_version"]
        elif dvc["data_type"] == "job":
            temp_dict["previous_data_version_job"] = dvc["previous_data_version"]
            temp_dict["new_data_version_job"] = dvc["new_data_version"]
        elif dvc["data_type"] == "all":
            temp_dict["previous_data_version"] = dvc["previous_data_version"]
            temp_dict["new_data_version"] = dvc["new_data_version"]
            temp_dict["previous_data_version_job"] = dvc["previous_data_version"]
            temp_dict["new_data_version_job"] = dvc["new_data_version"]
        
        if _id in data_version_changes_dict:
            data_version_changes_dict[_id].update(temp_dict)
        else:
            data_version_changes_dict[_id] = temp_dict
    
    # process for each company_datasource, since each has different data_version associated
    num_company = len(case_study_payload['company_datasources'])
    event = "finish"
    error_text = ""
    start_time = datetime.utcnow()
    i = 0
    for company_datasource_dict in case_study_payload['company_datasources']:
        i += 1
        try:
            # extract renlp and version info
            company_datasource_id = company_datasource_dict['company_datasource_id']
            re_nlp = company_datasource_dict['re_nlp']
            
            # get data_version_from and data_version_to
            # data_version_from = data_version_changes_dict[company_datasource_id]["previous_data_version"]
            data_version_to = data_version_changes_dict[company_datasource_id]["new_data_version"]
            data_version_job_from = data_version_changes_dict[company_datasource_id].get("previous_data_version_job", 0)
            data_version_job_to = data_version_changes_dict[company_datasource_id].get("new_data_version_job", 0)

            # extract nlp info for MC
            nlp_type = company_datasource_dict['nlp_type']
            nlp_pack = company_datasource_dict['nlp_pack']
            
            if nlp_type.strip().lower() == 'voc':
                # process nlp for voc reviews
                review_process_starts_at = datetime.utcnow()
                review_nlp_result_statuses = process_voc_reviews_for_company_datasource(
                    company_datasource_id=company_datasource_id,
                    data_version=data_version_to,
                    nlp_type=nlp_type,
                    nlp_pack=nlp_pack,
                    re_nlp=re_nlp,
                    logger=logger
                )
                review_process_finishes_at = datetime.utcnow()
                num_total_reviews = len(review_nlp_result_statuses)
                num_processed_reviews = sum(review_nlp_result_statuses)
                logger.info(f"Total reviews: {num_total_reviews} - processed: {num_processed_reviews} - "
                            f"took: {review_process_finishes_at - review_process_starts_at}")
                # for keeping track of every nlp process run
                helpers.insert_nlp_statistics_to_bigquery(
                    table_id=config.GCP_BQ_TABLE_VOC_NLP_STATISTICS,
                    request_id=request_id,
                    case_study_id=case_study_id,
                    company_datasource_id=company_datasource_id,
                    processed_at=review_process_starts_at,
                    finished_at=review_process_finishes_at,
                    company_id=company_datasource_dict['company_id'],
                    company_name=company_datasource_dict['company_name'],
                    source_id=company_datasource_dict['source_id'],
                    source_name=company_datasource_dict['source_name'],
                    num_total_reviews=num_total_reviews,
                    num_processed_reviews=num_processed_reviews,
                    nlp_type=company_datasource_dict['nlp_type'],
                    nlp_pack=company_datasource_dict['nlp_pack'],
                    data_version=data_version_to,
                    logger=logger
                )

            elif nlp_type.strip().lower() == 'voe':
                # process nlp for voe reviews
                review_process_starts_at = datetime.utcnow()
                review_nlp_result_statuses = process_voe_reviews_for_company_datasource(
                    company_datasource_id=company_datasource_id,
                    data_version=data_version_to,
                    nlp_type=nlp_type,
                    nlp_pack=nlp_pack,
                    re_nlp=re_nlp,
                    logger=logger
                )
                review_process_finishes_at = datetime.utcnow()
                num_total_reviews = len(review_nlp_result_statuses)
                num_processed_reviews = sum(review_nlp_result_statuses)
                logger.info(f"Total reviews: {num_total_reviews} - processed: {num_processed_reviews} - "
                            f"took: {review_process_finishes_at - review_process_starts_at}")
                # for keeping track of every nlp process run
                helpers.insert_nlp_statistics_to_bigquery(
                    table_id=config.GCP_BQ_TABLE_VOE_NLP_STATISTICS,
                    request_id=request_id,
                    case_study_id=case_study_id,
                    company_datasource_id=company_datasource_id,
                    processed_at=review_process_starts_at,
                    finished_at=review_process_finishes_at,
                    company_id=company_datasource_dict['company_id'],
                    company_name=company_datasource_dict['company_name'],
                    source_id=company_datasource_dict['source_id'],
                    source_name=company_datasource_dict['source_name'],
                    num_total_reviews=num_total_reviews,
                    num_processed_reviews=num_processed_reviews,
                    nlp_type=company_datasource_dict['nlp_type'],
                    nlp_pack=company_datasource_dict['nlp_pack'],
                    data_version=data_version_to,
                    logger=logger
                )
                
                # process nlp for voe jobs
                job_process_starts_at = datetime.utcnow()
                job_result_statuses = process_voe_jobs_for_company_datasource(
                    company_datasource_id=company_datasource_id,
                    data_version=data_version_job_to,
                    re_classify=False,
                    logger=logger
                )
                job_process_finishes_at = datetime.utcnow()
                num_total_jobs = len(job_result_statuses)
                num_processed_jobs = sum(job_result_statuses)
                logger.info(f"Total jobs: {num_total_jobs} - classified: {num_processed_jobs} - "
                            f"took: {job_process_finishes_at - job_process_starts_at}")

            else:
                raise Exception(f"Invalid nlp_type {nlp_type}")
            
        except Exception as e:
            event = "fail"
            error_text = f'{e.__class__.__name__}: {e.args}'
            logger.exception(f"[handle_task] error happened while processing "
                             f"company_datasource_id {company_datasource_id}")
        
        finally:
            progress = min(i / num_company, 0.95)
            helpers.update_worker_progress(payload=payload, progress=progress, logger=logger)
    
    logger.info(f"[handle_task] nlp completed successfully in {datetime.utcnow() - start_time}")
    # finish, announce aftertask
    time.sleep(10)
    aftertask_payload = {
        "type": postprocess_type,
        "event": event,
        "case_study_id": case_study_id,
        "request_id": request_id,
        "postprocess_id": postprocess_id,
        "postprocess": postprocess_object,
        "error": error_text,
    }
    pubsub_util.publish(
        gcp_product_id=config.GCP_PROJECT_ID,
        topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
        payload=aftertask_payload,
        logger=logger,
    )
        

if __name__ == "__main__":
    pubsub_util.subscribe(
        logger, 
        config.GCP_PROJECT_ID, 
        config.GCP_PUBSUB_SUBSCRIPTION, 
        handle_task
    )
