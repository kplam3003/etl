import sys
sys.path.append('../')

from datetime import datetime
import math
import time

import numpy as np
from google.cloud import bigquery

from core import pubsub_util, logger
from core.logger import init_logger
import config
from source import (
    Corpus,
    compute_tf_matrix,
    compute_tfidf
)

# initialization
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def update_progress(payload,
                    worker_progress,
                    project_id,
                    progress_topic):
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


def collect_keywords_from_table(keywords_output_table_id,
                                summary_table_prefix,
                                case_study_id, 
                                run_id):
    """
    Get keywords from BigQuery with given case_study_id and run_id
    """
    try:
        logger.info(f'[COLLECT] Collecting reviews from keywords_output...')
        statement = f"""
            WITH summary_table AS (
                SELECT DISTINCT
                    case_study_id,
                    run_id,
                    dimension_config_id,
                    review_id
                FROM `{summary_table_prefix}`
                WHERE _TABLE_SUFFIX = '{case_study_id}'
                    AND run_id = '{run_id}'
            ),
            keywords_table AS (
                SELECT *
                FROM `{keywords_output_table_id}`
                WHERE case_study_id = {case_study_id}
            )
            SELECT
                L.case_study_id,
                R.run_id,
                R.dimension_config_id,
                L.review_id,
                L.trans_review,
                L.keywords
            FROM keywords_table L
            INNER JOIN summary_table R
                ON L.case_study_id = R.case_study_id
                AND L.review_id = R.review_id
            ;
        """
        client = bigquery.Client()
        query_job = client.query(statement)
        query_results = query_job.result()
        # check query results
        assert query_results.total_rows != 0, "Query results in 0 records!"
        
        return query_results
    
    except Exception as error:
        logger.error(f"[COLLECT] Cannot query reviews for case_study_id={case_study_id}, run_id={run_id}, error={error}")
        raise Exception


def build_corpus(rows_iterator, build_batch_size):
    """
    Build a Corpus object incrementally:
    - stream and accumulate rows from BigQuery table
    - if a batch_size is met, add to corpus until no rows left
    - build vocabulary
    """
    # build initial corpus without any document
    num_items = rows_iterator.total_rows
    num_batch = math.ceil(num_items / build_batch_size)
    corpus = Corpus()
    logger.info(f'[BUILD_CORPUS] Building corpus: {num_items} documents in {num_batch} batches of size {build_batch_size}')
    
    temp_document_dict = {}
    current_batch = 0
    for i, row in enumerate(rows_iterator):
        # temporary storage. Will be reset after each batch
        temp_document_dict.update({
            row.review_id: {
                'text': row.trans_review,
                'keywords': [kw.strip() for kw in row.keywords.split(',')]
            }
        })
        # process when accumulate full batch or nothing left
        if (i + 1) % build_batch_size == 0 or (i + 1) == num_items:
            current_batch += 1
            logger.info(f'[BUILD_CORPUS] Accumulating batch {current_batch}/{num_batch}...')
            corpus.add_documents(temp_document_dict, rebuild_vocab=False)
            
            # reset temp storage
            del temp_document_dict
            temp_document_dict = {}
            
    # build vocab
    logger.info(f'[BUILD_CORPUS] Building vocabulary...')
    corpus.build_vocabulary()
    corpus.add_metadata('dimension_config_id', row.dimension_config_id)
    logger.info(f'[BUILD_CORPUS] Build done, corpus size: {corpus.corpus_size}; vocab_size: {corpus.vocab_size}')
    return corpus


def compute_word_frequency(corpus, normalize=True):
    """
    Calculate relative word frequency for each keyword from queried set of keywords
    """
    # prepare corpus and keywords
    start_time = datetime.now()
    logger.info(f'[COMPUTE] Computing word frequency')    
    logger.info(f'[COMPUTE] Corpus size x vocab size: ({corpus.corpus_size}x{corpus.vocab_size})')
    keywords_as_indices = list(corpus._kw_dict.values())
    keywords_ngram = np.array(list(map(lambda k: len(k.split()), corpus.vocabs)))
    
    # computation
    tf_matrix = compute_tf_matrix(keywords_as_indices, corpus.vocab_size) # term frequency
    df_matrix, tfidf_matrix = compute_tfidf(tf_matrix, normalize=normalize) # normalized-tf-idf
    scaled_tfidf_matrix = tfidf_matrix * keywords_ngram
    
    # construct return data
    assert corpus.vocab_size == scaled_tfidf_matrix.size, "Vocab and word frequency size mismatch!"
    assert corpus.vocab_size == df_matrix.size, "Vocab and document frequency size mismatch!"
    
    word_frequency_dict = {}
    for i in range(corpus.vocab_size):
        word_frequency_dict.update({
            corpus.vocabs[i]: {
                'rows': df_matrix[i],
                'frequency': scaled_tfidf_matrix[i]
            }
        })
        
    logger.info(f'[COMPUTE] Finished computing word frequency in {datetime.now() - start_time}')
    return word_frequency_dict
    

def split_list_into_batches(lst, batch_size):
    # looping till length l
    for i in range(0, len(lst), batch_size): 
        yield lst[i:i + batch_size]


def load_word_frequency_to_table(word_frequency_dict, load_batch_size, table_id, case_study_id, run_id, dimension_config_id,):
    """
    Load keyword results to bigquery table
    """
    # constructing item list
    logger.info('[LOAD] Constructing data for loading...')
    item_list = []
    for kw, metrics in word_frequency_dict.items():
        item_list.append({
            "case_study_id": case_study_id,
            "run_id": run_id,
            'dimension_config_id': dimension_config_id,
            "keywords": kw,
            'rows': int(metrics['rows']),
            "frequency": metrics['frequency'],
        })
    
    # create client
    logger.info(f"[LOAD] Loading keywords to table {table_id}...")
    client = bigquery.Client()
        
    # loading to bigquery in batches
    load_batches = split_list_into_batches(item_list, load_batch_size)
    
    num_row = len(item_list)
    num_batch = math.ceil(num_row / load_batch_size)
    logger.info(f'[LOAD] Will load {num_row} rows in {num_batch} batches, each of size {load_batch_size}')
        
    # iterating 
    for i, batch in enumerate(load_batches):
        try:
            logger.info(f'[LOAD] === Loading batch {i + 1}/{num_batch}, size {len(batch)} ===')
            # load batch to bigquery
            errors = client.insert_rows_json(json_rows=batch,
                                             table=table_id)
            if errors:
                logger.error("[LOAD] Encountered errors while inserting rows: {}".format(errors))
                
        except Exception as e:
            logger.exception(f"[LOAD] Cannot load batch {i + 1}, skipping...")
            
    logger.info(f"[LOAD] Load job finished: table_name={table_id}, num_items={len(item_list)}, first_item={item_list[0]}")
    
    return True


def publish_when_done(case_study_id, request_id, run_id, event, postprocess_object, error_text, 
                        project_id, after_task_topic):
    payload = {
        "type": "word_frequency",
        "event": event,
        "postprocess": postprocess_object,
        "case_study_id": case_study_id,
        "request_id": request_id,
        "run_id": run_id,
        "error": error_text,
    }
    # return payload
    pubsub_util.publish(logger,
                        project_id,
                        after_task_topic,
                        payload)
    return payload


def handle_task(payload, set_terminator=None):
    """
    Main interface of the worker
    """ 
    try:
        logger.info(f"[MAIN] Payload received: {payload}")
        logger.info(f"[MAIN] Begin word frequency event...")
        # update initial progress
        update_progress(payload=payload,
                        worker_progress=0.05,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS)
        # get infos from payload
        start_time = datetime.now()
        case_study_id = payload['case_study_id']
        request_id = payload['request_id']
        run_id = payload['run_id']
        nlp_type = payload['payload']['dimension_config']['nlp_type'].lower()
        postprocess_object = payload['postprocess']
        
        assert payload['type'] == 'word_frequency', "[MAIN] Wrong type! Expected word_frequency"

        # start processing
        # collect reviews
        if nlp_type == 'voc':
            summary_table_prefix = config.GCP_BQ_TABLE_VOC_SUMMARY_TABLE_PREFIX
            keywords_output_table_id = config.GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT
        elif nlp_type == 'voe':
            summary_table_prefix = config.GCP_BQ_TABLE_VOE_SUMMARY_TABLE_PREFIX
            keywords_output_table_id = config.GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT
        elif nlp_type == 'hra':
            logger.info(f"[MAIN] HRA case studies does not require word frequency. Will exit.")
            event = 'finish'
            error_text = ''
            return
        else:
            raise ValueError(f"No nlp_type {nlp_type}. Must be either 'voc' or 'voe' or 'hra'.")
            
        rows_iterator = collect_keywords_from_table(keywords_output_table_id=keywords_output_table_id,
                                                    summary_table_prefix=summary_table_prefix,
                                                    case_study_id=case_study_id,
                                                    run_id=run_id)
        # update progress
        update_progress(payload=payload,
                        worker_progress=0.3,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS)
        
        # build corpus incrementally
        corpus = build_corpus(rows_iterator, build_batch_size=config.CORPUS_BUILD_BATCH_SIZE)
        dimension_config_id = corpus.metadata['dimension_config_id']
        # update progress
        update_progress(payload=payload,
                        worker_progress=0.6,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS)
        
        # compute word frequency
        word_frequency_dict = compute_word_frequency(corpus, normalize=config.NORMALIZE)
        # update progress
        update_progress(payload=payload,
                        worker_progress=0.8,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS)
        
        # load result to table
        load_word_frequency_to_table(word_frequency_dict,
                                     load_batch_size=config.GCP_BQ_LOAD_BATCH_SIZE,
                                     table_id=config.GCP_BQ_TABLE_WORD_FREQUENCY,
                                     case_study_id=case_study_id,
                                     run_id=run_id,
                                     dimension_config_id=dimension_config_id)
        
        event = "finish"
        error_text = ""        
        logger.info(f"[MAIN] Keyword extraction completed successfully in {datetime.now() - start_time}!")
        
        
    except Exception as error:
        event = "fail"
        error_text = f'{error.__class__.__name__}: {error.args}'
        logger.exception(f"[MAIN] Error happens")
        logger.info(f"[MAIN] Keyword extraction failed in {datetime.now() - start_time}!")
    
    finally:
        # Publishing to after_task
        time.sleep(config.SLEEP)
        after_task_payload = publish_when_done(case_study_id=case_study_id,
                                               request_id=request_id, 
                                               run_id=run_id, 
                                               event=event, 
                                               error_text=error_text, 
                                               postprocess_object=postprocess_object,
                                               project_id=config.GCP_PROJECT_ID,
                                               after_task_topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK)

if __name__ == "__main__":
    """
    subscription payload structure:
    {
        "type": "word_frequency",
        "case_study_id": 250,
        "request_id": 321,
        "postprocess": {},
        "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004"
    }
    
    after_task payload structure
    {
        "type": "word_frequency",
        "event": "finish",  // finish, fail
        "postprocess": {},
        "case_study_id": 250,
        "request_id": 321,
        "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004",
        "error": "",  // if the event is fail
    }
    
    BIG PAYLOAD
    {
        "type": "word_frequency",
        "case_study_id": 244,
        "request_id": 245,
        "status": "finished",
        "run_id": "9706cd76-9901-11eb-9b8d-0242ac190008",
        "postprocess": {
            "postprocess_id": 574,
            "request_id": 245,
            "case_study_id": 244,
            "progress": 0,
            "postprocess_type": "word_frequency",
            "postprocess_error": "",
            "meta_data": {
                "run_id": "9706cd76-9901-11eb-9b8d-0242ac190008",
                "version": "1.0"
            },
            "status": "waiting",
            "created_at": "2021-04-09T08:22:07.060343",
            "updated_at": "2021-04-09T08:22:07.060350"
        }
    }
    """
    
    # handle_task({
    #     "type": "word_frequency",
    #     "case_study_id": 250,
    #     "request_id": 321,
    #     "postprocess": {},
    #     "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004"
    # })

    
    pubsub_util.subscribe(logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task)