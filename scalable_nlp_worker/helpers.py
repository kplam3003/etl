import time
import json
import sys
import requests
from urllib import parse

sys.path.append('../')

import meaningcloud
from google.cloud import bigquery

import config
from core import pubsub_util


DELIMITER = '\t' 
TIMEOUT = 120

MC_MODEL_LIST = [
    'VoC-Generic_en',
    'VoC-Banking_en' ,
    'VoC-Insurance_en', 
    'VoC-Retail_en',
    'VoC-Telco_en',
    'VoE-Performance_en',
    'VoE-Organization_en',
    'VoE-ExitInterview_en'
]

MC_NLP_PACK_MAP = {
    'VoC Generic': 'VoC-Generic',
    'VoC Banking': 'VoC-Banking',
    'VoC Insurance': 'VoC-Insurance',
    'VoC Retail': 'VoC-Retail',
    'VoC Telco': 'VoC-Telco',
    'VoE Performance Review': 'VoE-Performance',
    'VoE Organization': 'VoE-Organization',
    'VoE Exit Interview': 'VoE-ExitInterview',
    'Performance Review': 'VoE-Performance',
    'Organization': 'VoE-Organization',
    'Exit Interview': 'VoE-ExitInterview'
}

LOCAL_NLP_PACK_MAP = {
    'VoC Generic': 'voc_generic',
    'VoC Banking': 'voc_banking',
    'VoC Insurance': 'voc_insurance',
    'VoC Retail': 'voc_retail',
    'VoC Telco': 'voc_telco',
    # 'VoE Performance Review': 'VoE-Performance',
    # 'VoE Organization': 'VoE-Organization',
    # 'VoE Exit Interview': 'VoE-ExitInterview',
    # 'Performance Review': 'VoE-Performance',
    # 'Organization': 'VoE-Organization',
    # 'Exit Interview': 'VoE-ExitInterview'
}


def _make_MC_nlp_requests(text, model="VoC-Banking_en"):
    """
    Make a request to MC NLP API and parse the resulting response
    """
    assert model in MC_MODEL_LIST, f"Unsupported model {model}"
    req = meaningcloud.DeepCategorizationRequest(
            config.MC_LICENSE, 
            model,
            txt=text, 
            url=None, 
            doc=None, 
            polarity='y', 
            otherparams= {'verbose':'y'}, 
            extraheaders=None, 
            server=config.MC_API_URL
        )
    req._timeout = config.MC_TIMEOUT
    
    response = meaningcloud.DeepCategorizationResponse(
        req.sendReq()
    )

    if not response.isSuccessful():
        raise Exception(response.getStatusMsg())
    
    def _convert(category):
        result = {
            **category,
            "dimension": category.get('code').split('>')[0],
            "terms": list(map(lambda t: t.get('form'), category.get('term_list')))
        }
        return result
    
    categories = response.getCategories()
    results = list(map(_convert, categories))
    return results


def _do_MC_nlp(text, nlp_pack, nlp_lang, ref_id, logger):
    """
    Process text using Meaning Cloud NLP API
    """
    # check nlp pack first, and exit if not matched to any known packs
    nlp_pack = MC_NLP_PACK_MAP.get(nlp_pack.strip(), False)
    if not nlp_pack:
        logger.error(f"[do_MC_nlp] Invalid nlp pack: {nlp_pack}")
        return {
            "status": False
        }
    
    for wait in [0.5, 1, 2, 4]:
        try: 

            if text == None or len(text) == 0:
                return {
                    "status": False
                }

            model_str = "{nlp_pack}_{nlp_lang}".format(nlp_pack=nlp_pack, nlp_lang= nlp_lang)
            return {
                "status": True,
                "data": _make_MC_nlp_requests(text, model_str),
                "ref_id": ref_id
            }
        except Exception as error:
            logger.exception(f"[do_MC_nlp] error happened, will retry after {wait} seconds...")
            time.sleep(wait)
                       
    logger.error(f"[do_MC_nlp] failed after 4 tries")        
    return {
        "status": False
    }


def _transform_MC_nlp_result(nlp_result_item):
    """
    Transform nlp result from NLP 
    keys after transform are:
    ('code', 'dimension', 'label', 'polarity', 'terms', 'abs_relevance', 'rel_relevance')
    """
    # init empty item
    transformed_item = {}
    # simple keys
    transformed_item['code'] = nlp_result_item['code']
    transformed_item['dimension'] = nlp_result_item['dimension']
    transformed_item['label'] = nlp_result_item['label']
    transformed_item['polarity'] = nlp_result_item['polarity']
    
    # nested and to-be-transformed keys
    # terms are stored as a string separated by \t
    if nlp_result_item['terms']:
        nlp_result_item['terms'] = list(map(str, nlp_result_item['terms']))
        transformed_item['terms'] = '\t'.join(nlp_result_item['terms'])
    else:
        transformed_item['terms'] = nlp_result_item['terms']
    # abs relevance
    if nlp_result_item['abs_relevance']:
        transformed_item['relevance'] = float(nlp_result_item['abs_relevance'])
    else:
        transformed_item['relevance'] = None
    # relative relevance
    if nlp_result_item['relevance']:
        transformed_item['rel_relevance'] = float(nlp_result_item['relevance'])
    else:
        transformed_item['rel_relevance'] = None
        
    return transformed_item


def do_MC_nlp_on_review_doc(doc, nlp_type, nlp_pack, logger):
    try:
        nlp_lang = "en"
        ref_id = doc['review_id']
        if bool(doc['trans_status']):
            try:
                result_nlp = _do_MC_nlp(doc['trans_review'], nlp_pack, nlp_lang, ref_id, logger)
                if result_nlp["status"]:
                    nlp_results = result_nlp["data"]
                    ref_id = result_nlp["ref_id"]
                else:
                    nlp_results = None
            except Exception as error:
                logger.exception(f"[transform_{nlp_type.lower()}_review_item] error happened while doing NLP")

        if not nlp_results:
            logger.warning(f"[transform_{nlp_type.lower()}_review_item] nlp_results empty "
                           f"for nlp_pack {nlp_pack}; review text: {doc['trans_review']}")
            nlp_results = [{
                'code': None,
                'dimension': None,
                'label': None,
                'terms': None,
                'relevance': None,
                'abs_relevance': None,
                'polarity': None,
            }]
        
    except Exception as error:
        logger.exception(f"[transform_{nlp_type.lower()}_review_item] error happened")
        nlp_results = [{
            'code': None,
            'dimension': None,
            'label': None,
            'terms': None,
            'relevance': None,
            'abs_relevance': None,
            'polarity': None,
        }]
        
    transformed_nlp_results = []
    for item in nlp_results:
        transformed_nlp_results.append(
            _transform_MC_nlp_result(item)
        ) 
    return transformed_nlp_results


def _make_job_classification_request(job_title, conf_threshold=None, logger=None):
    """
    make a request for job type classification to prediction_service api
    """
    # mandatory
    job_title_param = f"job_title={parse.quote(job_title)}"
    # optional
    if isinstance(conf_threshold, float):
        conf_threshold_param = f"&conf_thresh={conf_threshold}"
    else:
        conf_threshold_param = ''
    # construct request
    request_url = f"{config.JOB_CLASSIFIER_API}/predict?{job_title_param}{conf_threshold_param}"
    
    # try 4 times
    waits = [0.5, 1, 2, 4]
    for wait in waits:
        try:
            # make request
            response = requests.post(request_url)
            status_code = response.status_code
            prediction_dict = json.loads(response.text)
            return prediction_dict
        
        except Exception as error:
            logger.exception(f"[classify_job_title] error happened, will retry in {wait} seconds. Server response code {status_code}")
            time.sleep(wait)
    
    logger.error(f"[classify_job_title]: Fail after {len(waits)} retries!")
    
    return {'prediction': 'Undefined', 'confidence': 0.0}


def classify_job_name_from_doc(doc, logger):
    # get feature, process, and make prediction
    job_title = doc['job_name']
    
    # catch not string
    if not isinstance(job_title, str):
        job_function = 'Undefined'
        confidence = 0.0
    # catch empty string
    elif job_title.strip() == '':
        job_function = 'Undefined'
        confidence = 0.0
    # request for prediction
    else:
        # make request to prediction service, job classifier
        prediction_result = _make_job_classification_request(
            job_title=job_title,
            conf_threshold=config.JOB_CLASSIFIER_CONFIDENCE_THRESHOLD,
            logger=logger
        )
        if not isinstance(prediction_result, dict):
            logger.error(f"[App.transform.transform_func]: prediction_result is not a dict: {prediction_result}")
            job_function = 'Undefined'
            confidence = 0.0
        else:
            job_function = prediction_result.get('prediction', 'Undefined')
            confidence = prediction_result.get('confidence', 0.0)
       
    return job_function


def _make_local_nlp_request(text, nlp_type, nlp_pack, logger):
    """
    make a request for ABSA pipeline to local NLP API
    """
    # make request
    request_headers = {
        "secret": config.TPP_NLP_SECRET
    }
    request_params = {
        "nlp_pack": LOCAL_NLP_PACK_MAP[nlp_pack],
        "conf_thres": 0.6,
    }
    request_data = {
        "text": text,
    }
    response = requests.post(
        config.TPP_NLP_API_URL,
        headers=request_headers,
        params=request_params,
        json=request_data
    )
    status_code = response.status_code
    predicted_dimensions = json.loads(response.text)
    
    return status_code, predicted_dimensions


def do_local_nlp_on_review_doc(doc, nlp_type, nlp_pack, logger):
    """
    Do NLP using local NLP service on review doc
    """
    nlp_results = [{
        'code': None,
        'dimension': None,
        'label': None,
        'terms': None,
        'relevance': None,
        'rel_relevance': None,
        'polarity': None,
    }]
    
    
    try:
        nlp_lang = "en"
        ref_id = doc['review_id']
        if bool(doc['trans_status']):
            try:
                status_code, nlp_results = _make_local_nlp_request(
                    doc['trans_review'],
                    nlp_type, 
                    nlp_pack, 
                    logger
                )
                if status_code != 200:
                    nlp_results = []
            except Exception as error:
                logger.exception(f"[transform_{nlp_type.lower()}_review_item] error happened while doing NLP")

        if not nlp_results:
            logger.warning(f"[transform_{nlp_type.lower()}_review_item] nlp_results empty "
                           f"for nlp_pack {nlp_pack}; review text: {doc['trans_review']}")
            final_nlp_results = [{
                'code': None,
                'dimension': None,
                'label': None,
                'terms': None,
                'relevance': None,
                'rel_relevance': None,
                'polarity': None,
            }]
            
        else:
            final_nlp_results = []
            for res in nlp_results:
                # adjectives = res["adjectives"]
                item = {
                    'code': res['code'],
                    'dimension': res['dimension'],
                    'label': res['label'],
                    'terms': '\t'.join(res['terms']),
                    'relevance': 10,
                    'rel_relevance': 100,
                    'polarity': res['polarity'],
                }
                final_nlp_results.append(item)
        
    except Exception as error:
        logger.exception(f"[transform_{nlp_type.lower()}_review_item] error happened")
        final_nlp_results = [{
            'code': None,
            'dimension': None,
            'label': None,
            'terms': None,
            'relevance': None,
            'rel_relevance': None,
            'polarity': None,
        }]
        
    finally:
        return final_nlp_results


def update_worker_progress(payload, progress, logger):
    """
    Helper function to publish worker's progress
        
    """
    case_study_id = payload['case_study_id']
    request_id = payload['postprocess']['request_id']
    postprocess_id = payload['postprocess']['postprocess_id']
    worker_type = payload['postprocess']['postprocess_type']
    
    progress_payload = {
        "type": worker_type,
        "event": "progress",
        "request_id": request_id,
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_id,
        "progress": progress
    }
    pubsub_util.publish(
        logger=logger,
        gcp_product_id=config.GCP_PROJECT_ID,
        topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
        payload=progress_payload
    )
    return True


def insert_nlp_statistics_to_bigquery(
    table_id,
    request_id,
    case_study_id,
    company_datasource_id,
    processed_at,
    finished_at,
    company_id,
    company_name,
    source_id,
    source_name,
    num_total_reviews,
    num_processed_reviews,
    nlp_type,
    nlp_pack,
    data_version,
    logger
):
    """
    Load NLP statistics into bigquery table
    """
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    row_dict = {
        "request_id": request_id,
        "case_study_id": case_study_id,
        "company_datasource_id": company_datasource_id,
        "processed_at": processed_at.__str__(),
        "finished_at": finished_at.__str__(),
        "company_id": company_id,
        "company_name": company_name,
        "source_id": source_id,
        "source_name": source_name,
        "num_total_reviews": num_total_reviews,
        "num_processed_reviews": num_processed_reviews,
        "nlp_type": nlp_type,
        "nlp_pack": nlp_pack,
        "data_version": data_version
    }
    errors = client.insert_rows_json(
        json_rows=[row_dict],
        table=table_id
    )
    if errors:
        logger.error(f"Error happened while inserting nlp statistics into {table_id} - "
                     f"item: {row_dict} - error: {errors}")
    else:
        logger.info(f"nlp statistics inserted successfully, destination: {table_id} - "
                    f"item: {row_dict}")
