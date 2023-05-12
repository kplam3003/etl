import sys
sys.path.append('../')

from datetime import datetime
import math
import time

from typing import List
from google.cloud import bigquery

from core import pubsub_util, logger
from core.logger import init_logger
import config
import helpers
from source import (
    initialize_spacy_pipeline,
    parse_noun_chunks, 
    parse_adjective_chunks,
    light_preprocess
)
from custom_dimension import (
    get_user_defined_dimension_labels,
    extract_user_defined_dimension_batch,
    load_user_defined_dimension_to_table
)

# initialization
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def update_progress(
    payload,
    worker_progress,
    project_id,
    progress_topic
):
    """
    Helper function to publish worker's progress
    
    Param
    -----
    payload: Dict
        Payload for export worker
        
    worker_progress: Float
        Current progress of the worker. Maximum is 1.0.
        
    project_id: String
        GCP project id
    
    progress_topic: String
        Pubsub topic for progress to be sent
        
    Return
    ------
    None
        
    """
    request_id = payload['postprocess']['request_id']
    postprocess_id = payload['postprocess']['postprocess_id']
    worker_type = payload['postprocess']['postprocess_type']
    case_study_id = payload['postprocess']['case_study_id']
    
    progress_payload = {
        "type": worker_type,
        "event": "progress",
        "request_id": request_id,
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_id,
        "progress": worker_progress
    }
    pubsub_util.publish(logger,
                        project_id,
                        progress_topic,
                        progress_payload)
    return True


def collect_reviews_from_summary_table(staging_table_id, case_study_id):
    """
    Get reviews from BigQuery with given case_study_id and run_id
    """
    try:
        logger.info(f'[COLLECT] Collecting reviews from table {staging_table_id}...')
        statement = f"""
            SELECT
                created_at,
                review_id,
                source_name,
                company_name,
                nlp_pack,
                nlp_type,
                user_name,
                language,
                review,
                trans_review,
                trans_status,
                rating,
                batch_id,
                batch_name,	
                file_name,	
                review_date,
                company_id,
                source_id,
                step_id,
                request_id,
                case_study_id,
                parent_review_id,
                technical_type,
                review_country,
                ARRAY_AGG(
                    CASE WHEN polarity IS NULL THEN 'NONE'
                    ELSE polarity
                    END
                ) AS polarities
            FROM `{staging_table_id}`
            WHERE case_study_id = {case_study_id}
            GROUP BY
                created_at,
                review_id,
                source_name,
                company_name,
                nlp_pack,
                nlp_type,
                user_name,
                language,
                review,
                trans_review,
                trans_status,
                rating,
                batch_id,
                batch_name,	
                file_name,	
                review_date,
                company_id,
                source_id,
                step_id,
                request_id,
                case_study_id,
                parent_review_id,
                technical_type,
                review_country
            ;
        """
        client = bigquery.Client()
        query_job = client.query(statement)
        query_results = query_job.result()
        # check query result
        assert query_results.total_rows != 0, "Query results in 0 records!"
        
        return query_results
    
    except Exception as error:
        logger.error(f"[COLLECT] Cannot query reviews for case_study_id={case_study_id}, error={error}")
        raise Exception


def delete_keyword_data(keyword_output_table_id, case_study_id):
    """
    Delete keywords data for extraction to be re-done
    """
    try:
        logger.info(f'[DELETE] Deleting extracted keywords from case study {case_study_id}...')
        statement = f"""
            DELETE `{keyword_output_table_id}`
            WHERE case_study_id = {case_study_id}
        """
        client = bigquery.Client()
        query_job = client.query(statement)
        query_results = query_job.result()
        logger.info(f'[DELETE] Extracted keywords deleted!')
        
    except Exception as error:
        logger.error(f"[DELETE] Cannot delete extracted keywords from case study {case_study_id}, error={error}")
        raise

    
def case_study_is_parsed(keyword_output_table_id, case_study_id):
    """
    Check if reviews in case study has already been parsed by keyword extractor
    """
    try:
        logger.info(f'[CHECK] Checking if reviews has already been parsed...')
        statement = f"""
            SELECT COUNT(*) AS row_count
            FROM `{keyword_output_table_id}`
            WHERE case_study_id = {case_study_id}
        """
        client = bigquery.Client()
        query_job = client.query(statement)
        query_results = query_job.result()
        # check query result
        for row in query_results:
            if row.row_count != 0:
                # has reviews => already parsed once
                return True
            else:
                # no review => not parsed yet
                return False
    
    except Exception as error:
        logger.error(f"[CHECK] Cannot check keyword parse status for case_study_id={case_study_id}, error={error}")
        raise


def _parse_keywords_from_batch(batch, nlp, disable_components, mode=None):
    """
    Parse keywords for a given batch of texts using spacy batch pipeline
    """
    batch_length = len(batch)
    progress_multiplier = 0.2
    progress_report_at = int(batch_length * progress_multiplier)
    i = 0
   
    keywords = []
    for d in nlp.pipe(
        batch,
        disable=disable_components,
        batch_size=config.SPACY_BATCH_SIZE, 
        n_process=config.N_PROCESS
    ):
        _full_noun_chunks, _base_noun_chunks = parse_noun_chunks(d, return_base_chunk=True)
        _full_adj_chunks, _base_adj_chunks = parse_adjective_chunks(d, return_base_chunk=True)
        if mode == 'full':
            keywords.append(_full_noun_chunks + _full_adj_chunks)
        elif mode == 'base':
            keywords.append(_base_noun_chunks + _base_adj_chunks)
        else:
            keywords.append(_base_noun_chunks + _base_adj_chunks)
            
        # print progress for diagnose purpose
        i += 1
        if any([i == batch_length, 
                (i % progress_report_at) == 0]):
            logger.info(f'[PARSE] == {i}/{batch_length} reviews parsed! ==') 
            
    return keywords


def parse_keywords(
    reviews_dict_batch, 
    nlp, 
    disable_components=['ner'], 
    mode='base'
):
    """
    Parse keywords from given text
    """
    logger.info('[PARSE] Parsing keywords from reviews...')
    # prepare and preprocess texts
    review_ids = list(reviews_dict_batch.keys())
    review_texts = [light_preprocess(t) for t in list(reviews_dict_batch.values())]
    # parse
    if disable_components:
        logger.info(f'[PARSE] Pipeline components {disable_components} disabled!')
    start_time = datetime.now()
    keywords = _parse_keywords_from_batch(review_texts, 
                                          nlp=nlp, 
                                          disable_components=disable_components,
                                          mode=mode)
    # construct return dict
    reviews_dict_with_keywords = {}
    for i in range(len(keywords)):
        reviews_dict_with_keywords.update({
            review_ids[i]: {
                "text": review_texts[i],
                "keywords": keywords[i]
            }
        })
    
    logger.info(f'[PARSE] Finished parsing in {datetime.now() - start_time}')
    return reviews_dict_with_keywords


def load_keywords_to_table(reviews_dict_with_keywords, table_id, case_study_id):
    """
    Load keyword results to bigquery table
    """
    # constructing item list
    logger.info('[LOAD] Constructing data for loading...')
    item_list = []
    for rid, kwd in reviews_dict_with_keywords.items():
        item_list.append({
            "case_study_id": case_study_id,
            "review_id": rid,
            "trans_review": kwd['text'],
            "keywords": ', '.join(kwd['keywords'])
        })
    
    # load job
    logger.info(f"[LOAD] Loading keywords to table {table_id}...")
    client = bigquery.Client()
    errors = client.insert_rows_json(json_rows=item_list,
                                     table=table_id)
    if errors:
        logger.error("[LOAD] Encountered errors while inserting rows: {}".format(errors))
        raise Exception("[LOAD] Load job fails!")
    logger.info(f"[LOAD] Load job successful: table_name={table_id}, num_items={len(item_list)}, first_item={item_list[0]}")
    
    return True


def publish_when_done(
    case_study_id, 
    request_id, 
    event, 
    postprocess_object, 
    error_text, 
    project_id, 
    after_task_topic
):
    payload = {
        "type": "keyword_extract",
        "event": event,
        "postprocess": postprocess_object,
        "case_study_id": case_study_id,
        "request_id": request_id,
        "error": error_text,
    }
    # return payload
    pubsub_util.publish(
        logger,
        project_id,
        after_task_topic,
        payload
    )
    return payload


def handle_task(payload):
    """
    Main interface of the worker
    """
    try:
        logger.info(f"[MAIN] Payload received: {payload}")
        logger.info(f"[MAIN] Begin keyword extraction event...")
        # update initial progress
        update_progress(
            payload=payload,
            worker_progress=0.05,
            project_id=config.GCP_PROJECT_ID,
            progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS
        )
        # get infos from payload
        case_study_id = payload['case_study_id']
        nlp_type = payload['payload']['dimension_config']['nlp_type'].lower()
        request_id = payload['request_id']
        postprocess_object = payload['postprocess']
        force_rerun = payload['force']
        run_id = payload['run_id']

        # handle HRA case study
        if nlp_type == "hra":
            event = "finish"
            error_text = ""
            dimension_config: dict = payload["payload"]["dimension_config"]
            edu_dimension_config: dict = payload["payload"]["edu_dimension_config"]
            company_datasources: List[dict] = payload["payload"]["company_datasources"]
            
            # Check & Load `dwh.hra_casestudy_dimension_config`
            helpers.load_hra_casestudy_dimension_config(
                case_study_id=case_study_id, dimension_config=dimension_config, run_id=run_id
            )
            
            for idx, company_datasource in enumerate(company_datasources):
                helpers.process_hra_casestudy_jobfunction(
                    case_study_id=case_study_id,
                    company_datasource=company_datasource,
                    dimension_config=dimension_config,
                    run_id=run_id,
                    logger=logger,
                )
                helpers.process_hra_casestudy_education_degree(
                    case_study_id=case_study_id,
                    company_datasource=company_datasource,
                    dimension_config=edu_dimension_config,
                    run_id=run_id,
                    logger=logger,
                )
                update_progress(
                    payload=payload,
                    worker_progress=(idx+1)/len(company_datasources),
                    project_id=config.GCP_PROJECT_ID,
                    progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS
                )

            # all done for HRA, skip to `finally` clause
            return

        # infos for user-defined dimension extraction
        payload_dimension_config = payload['payload']['dimension_config']['dimensions']
        user_defined_dimensions = get_user_defined_dimension_labels(
            payload_dimensions=payload_dimension_config
        )
        payload_polarities = payload['payload']['polarities']
        polarity_mapping = {
            d['nlp_polarity']: d['modified_polarity']
            for d in payload_polarities
        }
        
        assert payload['postprocess']['postprocess_type'] == 'keyword_extract', "[MAIN] Wrong type, expected keyword_extract!"
        
        # get proper table id
        if nlp_type == 'voc':
            staging_table_id = config.GCP_BQ_TABLE_VOC
            keywords_output_table_id = config.GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT
            user_defined_dimension_table_id = config.GCP_BQ_TABLE_VOC_CUSTOM_DIMENSION
        elif nlp_type == 'voe':
            staging_table_id = config.GCP_BQ_TABLE_VOE
            keywords_output_table_id = config.GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT
            user_defined_dimension_table_id = config.GCP_BQ_TABLE_VOE_CUSTOM_DIMENSION
        else:
            raise ValueError(f"No nlp_type {nlp_type}. Must be either 'voc' or 'voe'.")
            
        # check if case study has been parsed once
        is_parsed = case_study_is_parsed(
            keyword_output_table_id=keywords_output_table_id,
            case_study_id=case_study_id
        )
        
        # if rerun is forced AND cs has already been parsed once => delete => proceed as normal
        # else if rerun is NOT forced but has already been parsed once => skip parsing
        # else if rerun is forced but NOT yet parsed, just parse => proceed as normal
        # else if rerun is NOT forced and NOT yet parsed => proceed as normal
        if force_rerun and is_parsed:
            delete_keyword_data(
                keyword_output_table_id=keywords_output_table_id,
                case_study_id=case_study_id
            )
            logger.info(f'[MAIN] Case study {case_study_id} will now be parsed for keywords.')
        elif is_parsed and not user_defined_dimensions:
            # a CS needs to be parsed only once
            logger.info(
                f'[MAIN] Case study {case_study_id} was already parsed for keywords '
                'and has no user-defined dimensions. Exiting...'
            )
            event = "finish"
            error_text = ""
            return
        
        # init spacy model
        nlp = initialize_spacy_pipeline(
            model_name=config.SPACY_MODEL,
            download=True,
            exclude_stopwords=config.EXCLUDE_STOPWORDS,
            include_stopwords=config.INCLUDE_STOPWORDS,
            logger=logger
        )
        
        # execution
        # get iterator
        rows_iterator = collect_reviews_from_summary_table(
            staging_table_id=staging_table_id,
            case_study_id=case_study_id
        )
        
        # update progress
        update_progress(
            payload=payload,
            worker_progress=0.2,
            project_id=config.GCP_PROJECT_ID,
            progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS
        )
        
        # process batches
        # prepare variables
        start_time = datetime.now()
        i_total = 0
        i_batch = 0
        current_batch = 0
        
        num_row = rows_iterator.total_rows
        num_batch = math.ceil(num_row / config.WORKER_BATCH_SIZE)
        # for progress
        start_progress = 0.2
        end_progress = 0.9
        step_progress = (end_progress - start_progress) / num_batch
        current_progress = 0.2
        
        logger.info(f'[MAIN] Will process {num_row} reviews in {num_batch} batches, each of size {config.WORKER_BATCH_SIZE}')
        
        temp_review_batch = {}
        temp_review_batch_for_extraction = []
        
        # iterating 
        event = "finish"
        error_text = ""
        for row in rows_iterator:
            i_total += 1
            i_batch += 1
            temp_review_batch.update({row.review_id: row.trans_review})
            temp_review_batch_for_extraction.append(row)
            
            if i_batch == config.WORKER_BATCH_SIZE or i_total == num_row:
                current_batch += 1
                # try to process, skip current batch if it fails
                try:
                    logger.info(f'[MAIN] === Processing batch {current_batch}/{num_batch} ===')
                    # extracting keywords, only when rerun is forced or CS has not been parsed
                    if force_rerun or not is_parsed:
                        # parse batch
                        temp_review_batch_with_keywords = parse_keywords(
                            temp_review_batch,
                            nlp=nlp,
                            disable_components=config.SPACY_DISABLE_COMPONENTS,
                            mode=config.EXTRACTION_MODE
                        )
                        # load batch to bigquery
                        load_keywords_to_table(
                            temp_review_batch_with_keywords,
                            table_id=keywords_output_table_id,
                            case_study_id=case_study_id
                        )
                    
                    # extracting custom dimensions
                    # only extract dimensions if there is actually user-defined dimensions
                    if user_defined_dimensions:
                        for udd in user_defined_dimensions:
                            user_defined_extractions = extract_user_defined_dimension_batch(
                                row_batch=temp_review_batch_for_extraction,
                                dimension_name=udd['dimension'],
                                label_name=udd['label'],
                                keywords=udd['keywords'],
                                polarity_mapping=polarity_mapping,
                                run_id=run_id,
                                logger=logger
                            )
                            
                        if user_defined_extractions:
                            load_user_defined_dimension_to_table(
                                table_id=user_defined_dimension_table_id,
                                user_defined_extractions=user_defined_extractions,
                                logger=logger
                            )
                        else:
                            logger.warning("[MAIN] No user-defined dimensions extracted!")
                        
        
                except Exception as error:
                    logger.exception(f"[MAIN] Cannot process batch {current_batch}, skipping...")
                    event = "fail"
                    error_text = f'{error.__class__.__name__}: {error.args}'
                finally:
                    # update progress after each batch, regardless of status
                    current_progress += step_progress
                    update_progress(
                        payload=payload,
                        worker_progress=current_progress,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS
                    )
                    # reset review container
                    temp_review_batch = {}
                    temp_review_batch_for_extraction = []
                    i_batch = 0
        
        logger.info(f"[MAIN] Keyword extraction completed in {datetime.now() - start_time}!")

    except Exception as error:
        event = "fail"
        error_text = f'{error.__class__.__name__}: {error.args}'
        logger.exception(f"[MAIN] Error happens")
        logger.info(f"[MAIN] Keyword extraction failed in {datetime.now() - start_time}!")
    
    finally:
        # Publishing to after_task
        time.sleep(config.SLEEP)
        after_task_payload = publish_when_done(
            case_study_id=case_study_id,
            request_id=request_id, 
            event=event, 
            error_text=error_text, 
            postprocess_object=postprocess_object,
            project_id=config.GCP_PROJECT_ID,
            after_task_topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK
        )


if __name__ == "__main__":
    """
    subscription/trigger payload structure:
    {
        "type": "keyword_extract",
        "case_study_id": 365,
        "request_id": 321,
        "postprocess": {"postprocess object, passed as-is"},
        "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004"
    }
    
    after_task payload structure
    {
        "type": "keyword_extract",
        "event": "finish",  # finish, fail
        "postprocess": {}, # postprocess object, passed as-is
        "case_study_id": 250,
        "request_id": 321,
        "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004",
        "error": "",  # if the event is fail
    }
    
    """
    # handle_task({
    #     "type": "keyword_extract",
    #     "case_study_id": 365,
    #     "request_id": 366,
    #     "postprocess": {
    #         "request_id": 123,
    #         "postprocess_id": 123,
    #         "postprocess_type": "keyword_extract"
    #     }, # postprocess object, keep as-is
    #     "payload": {
    #         "dimension_config": {
    #             "nlp_type": "VoC"
    #         }
    #     }
    # })
    
    pubsub_util.subscribe(logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task)
