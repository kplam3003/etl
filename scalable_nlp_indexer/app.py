import os
import sys
import time

sys.path.append('../')

from datetime import datetime
from google.cloud import bigquery

from core import pubsub_util, logger
from core.logger import init_logger
from core.operation import auto_killer
from elasticsearch import Elasticsearch, helpers


import config

# initialization
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def update_progress(payload,
                    worker_progress,
                    project_id,
                    progress_topic):
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

def prepare_elasticsearch_case_study_index(case_study_id, *args, **kwargs):
    nlp_index = f"{config.CASE_STUDY_NLP_INDEX_PREFIX}{case_study_id}"
    logger.info(f"[NLP Indexing - SUB] START prepare index {nlp_index}")
    try:
        logger.info(f"[NLP Indexing - SUB] START delete nlp_index {nlp_index}")
        es_client.indices.delete(index=nlp_index, ignore=[400, 404])
        logger.info(f"[NLP Indexing - SUB] END delete nlp_index {nlp_index}")

        logger.info(f"[NLP Indexing - SUB] START create index {nlp_index}")
        settings = {
            "mappings": {
                "properties": {
                    "review_id": {
                        "type": "keyword"
                    },
                    "parent_review_id": {
                        "type": "keyword"
                    },
                    "technical_type": {
                        "type": "keyword"
                    },
                    "source_name": {
                        "type": "keyword"
                    },
                    "company_name": {
                        "type": "keyword"
                    },
                    "case_study_id": {
                        "type": "long",
                    },
                    "case_study_name": {
                        "type": "text",
                    },
                    "language": {
                        "type": "text",
                    },
                    "nlp_pack": {
                        "type": "text"
                    },
                    "nlp_type": {
                        "type": "text"
                    },
                    "orig_review": {
                        "type": "text"
                    },
                    "trans_review": {
                        "type": "text"
                    },
                    "rating": {
                        "type": "text"
                    },
                    "dimension": {
                        "type": "keyword"
                    },
                    "review_date": {
                        "type": "date"
                    },
                    "label": {
                        "type": "keyword"
                    },
                    "terms": {
                        "type": "text"
                    },
                    "relevance": {
                        "type": "text"
                    },
                    "rel_relevance": {
                        "type": "text"
                    },
                    "polarity": {
                        "type": "keyword"
                    },
                    "sentiment_score": {
                        "type": "double"
                    },
                    "words_count": {
                        "type": "long"
                    },
                    "characters_count": {
                        "type": "long"
                    },
                    "review_country": {
                        "type": "keyword"
                    }
                }
            }
        }

        es_client.indices.create(index=nlp_index, body=settings)

        max_result_window = kwargs.get("max_result_window")
        if kwargs.get("max_result_window") and isinstance(max_result_window, int):
            dynamic_index_settings = {
                "index": {
                    "max_result_window": max_result_window
                }
            }

            es_client.indices.put_settings(
                index=nlp_index,
                body=dynamic_index_settings
            )

        logger.info(f"[NLP Indexing - SUB] Success index {nlp_index}")
    except Exception as error:
        logger.error(f"[NLP Indexing SUB] ERROR while delete old document in Elasticsearch with errors: {error}")
        raise error




def _prepare_bulk_data_for_es_nlp_index(nlp_index, dict_items):
    bulk_data = []
    for item in dict_items:
        item["_index"] = nlp_index
        bulk_data.append(item)

    return bulk_data


def bulk_es_nlp_index_review(case_study_id, items):
    nlp_index = f"{config.CASE_STUDY_NLP_INDEX_PREFIX}{case_study_id}"
    logger.info(f"[NLP Indexing - SUB] Start nlp_index {len(items)} items with nlp_index {nlp_index}")
    try:
        bulk_data = _prepare_bulk_data_for_es_nlp_index(nlp_index, items)
        helpers.bulk(client=es_client, actions=bulk_data)
    except Exception as error:
        logger.error(f"[NLP Indexing SUB] ERROR while nlp_indexing document in Elasticsearch with errors: {error}")
        raise error

    logger.info(f"[NLP Indexing - SUB] END nlp_index {len(items)} items with nlp_index {nlp_index}")


def load_data_into_es_for_nlp_indexing(
        payload,
        case_study_id,
        run_id,
        table_summary_table_prefix,
        table_casestudy_dimension_config,
        table_nlp_output_case_study,
        table_polarity_trans,
        table_language_trans,
        table_casestudy_company_source,
        table_batch_status,
        table_casestudy_batchid,
        table_parent_review_mapping,
        table_review_country_mapping
):

    sql_query = """
    WITH 
        BATCH_LIST as (
            SELECT
                batch_id
            FROM
                `{table_batch_status}`
            WHERE
                batch_id in (
                    SELECT
                        batch_id
                    FROM
                        `{table_casestudy_batchid}`
                    WHERE
                        case_study_id = {case_study_id}
                )
                AND status = 'Active'
        ),
        DIM_LIST as (
            SELECT nlp_dimension as dimension, nlp_label as label 
            FROM `{table_casestudy_dimension_config}` 
            WHERE case_study_id = {case_study_id}
                AND run_id = '{run_id}'
        ), 
        TEMP1 as (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                c.dimension_type,
                a.dimension,
                c.modified_dimension,
                # CASE WHEN c.nlp_dimension is null THEN a.dimension ELSE c.modified_dimension END modified_dimension,
                a.label,
                c.modified_label,
                # CASE WHEN c.nlp_label is null THEN a.label ELSE c.modified_label END modified_label,
                c.is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, nlp_dimension, modified_dimension, nlp_label, modified_label,
                    is_used, dimension_type
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
                AND  a.dimension = c.nlp_dimension
                AND a.label = c.nlp_label
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id = '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
            WHERE a.case_study_id = {case_study_id}
                AND a.dimension in (SELECT dimension FROM DIM_LIST) AND a.label in (SELECT label FROM DIM_LIST) 
                AND run_id = '{run_id}'            
        ),
        TEMP2 AS (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                c.dimension_type,
                a.dimension,
                c.modified_dimension,
                a.label,
                a.label as modified_label,
                cast(null as bool) as is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, is_used, dimension_type, nlp_dimension, modified_dimension
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
                AND a.dimension = c.nlp_dimension
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id =  '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
                            
            WHERE a.case_study_id = {case_study_id}
                AND a.dimension in (SELECT dimension FROM DIM_LIST) and a.label not in (SELECT label FROM DIM_LIST)
                AND run_id =  '{run_id}'
        ) ,TEMP3 AS (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                cast(null as string) as dimension_type,
                a.dimension,
                a.dimension as modified_dimension,
                a.label,
                a.label as modified_label,
                cast(null as bool) as is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, is_used, dimension_type
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id =  '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
                            
            WHERE a.case_study_id = {case_study_id}
                AND (a.dimension is null OR a.dimension not in (SELECT dimension FROM DIM_LIST))
                AND run_id =  '{run_id}'
        ), TOTAL AS (
            SELECT distinct * FROM TEMP1 
            UNION  ALL
            SELECT distinct * FROM TEMP2
            UNION  ALL
            SELECT distinct * FROM TEMP3
        ),
        parent_review AS (
            SELECT 
            DISTINCT 
            case_study_id,
            review_id,
            parent_review_id,
            technical_type
            FROM   `{table_parent_review_mapping}`
            WHERE
                    batch_id IN (
                        SELECT
                            batch_id FROM BATCH_LIST
                    )
                    AND case_study_id = {case_study_id}

        ),
        review_country_mapping AS (
            SELECT DISTINCT
                case_study_id,
                review_id,
                review_country
            FROM `{table_review_country_mapping}`
            WHERE
                batch_id IN (
                    SELECT batch_id FROM BATCH_LIST
                )
                AND case_study_id = {case_study_id}
        ),
        summary_data AS (
            SELECT
                created_at,
                a.case_study_id,
                case_study_name,
                source_id,
                source_name,
                company_id,
                company_name,
                nlp_pack,
                nlp_type,
                user_name,
                dimension_config_id,
                dimension_config_name,
                review_id,
                review,
                trans_review,
                trans_status,
                review_date,
                rating,
                CASE 
                    WHEN language_code is null or language_code = '' THEN 'blank'
                    ELSE language_code
                END as language_code,
                CASE 
                    WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
                    WHEN language is null AND language_code is not null THEN language_code
                    ELSE language
                END as language,
                code,
                dimension_type,
                dimension,
                modified_dimension,
                label,
                modified_label,
                is_used,
                terms,
                a.relevance,
                a.rel_relevance,
                polarity,
                modified_polarity,
                batch_id,
                batch_name,
                a.run_id,
                abs_relevance_inf,
                rel_relevance_inf
            FROM (SELECT DISTINCT * FROM TOTAL) a
            LEFT JOIN (
                SELECT 
                    DISTINCT 
                    case_study_id,
                    run_id, 
                    abs_relevance_inf, 
                    rel_relevance_inf 
                FROM `{table_casestudy_company_source}` a
                WHERE a.case_study_id = {case_study_id} AND run_id =  '{run_id}'
            ) b
            ON a.case_study_id = b.case_study_id AND a.run_id = b.run_id
            WHERE
                (CASE WHEN abs_relevance_inf > 0 THEN a.relevance >= abs_relevance_inf ELSE 1=1 END) AND 
                (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END)
        )
        SELECT 
            s.case_study_id,
            case_study_name,
            company_name,
            source_name,
            company_id, 
            source_id,
            language,
            nlp_pack,
            nlp_type,
            s.review_id,
            review as orig_review,
            trans_review,
            CASE
                WHEN LENGTH(trans_review)=0 THEN 0
                ELSE LENGTH(trans_review)-LENGTH(REPLACE(trans_review," ",""))+1
            END AS words_count,
            CHAR_LENGTH(trans_review) AS characters_count,
            rating,
            review_date,
            modified_dimension as dimension,
            modified_label as label,
            terms,
            relevance,
            rel_relevance,
            CASE 
                WHEN p.technical_type= "pros" AND polarity in ('N','N+','NEU') THEN 'P'
                WHEN p.technical_type= "cons" AND polarity in ('NEU','P','P+') THEN 'N'
                WHEN p.technical_type= "problems_solved" AND polarity in ('N','N+','NEU') THEN 'P'
                WHEN p.technical_type= "switch_reasons" AND polarity in ('N','N+','NEU') THEN 'P'
                ELSE polarity
            END AS polarity,
            p.parent_review_id,
            p.technical_type,
            modified_polarity as sentiment_score,
            CASE
                WHEN r.review_country IS NULL THEN 'blank'
                WHEN r.review_country = 'Unknown' THEN 'blank'
                ELSE r.review_country
            END AS review_country
        FROM summary_data s
        LEFT JOIN parent_review p
            ON s.review_id = p.review_id
        LEFT JOIN review_country_mapping r
            ON s.review_id = r.review_id
        WHERE dimension_config_name is not null;
        """.format(
        table_summary_table = f"{table_summary_table_prefix}_{case_study_id}",
        table_casestudy_dimension_config = table_casestudy_dimension_config,
        case_study_id = case_study_id,
        run_id = run_id,
        table_nlp_output_case_study = table_nlp_output_case_study,
        table_polarity_trans = table_polarity_trans,
        table_language_trans = table_language_trans,
        table_casestudy_company_source = table_casestudy_company_source,
        table_batch_status=table_batch_status,
        table_casestudy_batchid=table_casestudy_batchid,
        table_parent_review_mapping=table_parent_review_mapping,
        table_review_country_mapping=table_review_country_mapping,
    )

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        allow_large_results=True
    )

    query_job = client.query(sql_query, job_config=job_config)
    query_job.result()

    destination = query_job.destination
    destination = client.get_table(destination)
    page_size = config.NLP_INDEX_BATCH_SIZE

    rows = client.list_rows(destination, page_size=page_size, max_results=None)

    prepare_elasticsearch_case_study_index(case_study_id, max_result_window=rows.total_rows)

    process_items = 0
    pages_per_progress_announcement = 10
    page_counter = 0
    for page in rows.pages:
        items = []
        item_fields = [
            "review_id",
            "parent_review_id",
            "technical_type",
            "source_name",
            "company_name",
            "case_study_id",
            "case_study_name",
            "language",
            "nlp_pack",
            "nlp_type",
            "orig_review",
            "trans_review",
            "rating",
            "dimension",
            "review_date",
            "label",
            "terms",
            "relevance",
            "rel_relevance",
            "polarity",
            "sentiment_score",
            "words_count",
            "characters_count"
        ]

        for row in page:
            item_tmp = dict()
            for field in item_fields:
                item_tmp[field] = row[field]

            items.append(item_tmp)

        try:
            bulk_es_nlp_index_review(case_study_id, items)
            process_items += len(items)
            logger.info(f"[NLP Indexing - SUB] Success load {process_items}/{rows.total_rows} records into ES")
            page_counter += 1
            if page_counter == pages_per_progress_announcement:
                update_progress(
                    payload=payload,
                    worker_progress=min(0.90, round((process_items + 1)/ (rows.total_rows + 1), 2)),
                    project_id=config.GCP_PROJECT_ID,
                    progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS
                )
                page_counter = 0
                

        except Exception as error:
            logger.exception(f"[NLP Indexing - SUB] Error happens")
            logger.info(f"[NLP Indexing - SUB] Elasticsearch nlp_index failed at {datetime.now()}!")
            raise error

def publish_when_done(case_study_id, request_id, run_id, event, postprocess_object, error_text,
                        project_id, after_task_topic):
    payload = {
        "type": "nlp_index",
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

@auto_killer
def handle_task(payload, set_terminator=None):
    """
    Main interface of the worker
    """
    start_time = datetime.now()
    event = None
    error_text = ""
    # Set terminator for before kill execution

    try:
        update_progress(payload=payload,
                        worker_progress=0.0,
                        project_id=config.GCP_PROJECT_ID,
                        progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS)

        def _terminator():
            logger.info("Grateful terminating...")
            pubsub_util.publish(logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC, payload)
            logger.info(f"Republishing a message to {config.GCP_PUBSUB_TOPIC}: {payload}")
            os._exit(1)

        set_terminator(_terminator)
        
        ### Catching HRA case studies
        nlp_type = payload["payload"]["dimension_config"]["nlp_type"].lower()
        if nlp_type == "hra":
            logger.info(f"[MAIN] HRA case studies does not require NLP indexing. Will exit.")
            event = "finish"
            error_text = ""
            return

        logger.info(f"[NLP Indexing] Received: {payload}")
        logger.info(f"[NLP Indexing] Begin load NLP into Elasticsearch for nlp_indexing...")
        case_study_id = payload["case_study_id"]
        run_id = payload["run_id"]
        table_summary_table_prefix = ""
        table_case_study_dimension_config = ""
        table_nlp_output_case_study = ""
        table_polarity_trans = ""
        table_language_trans = ""
        table_case_study_company_source = ""
        table_batch_status = ""
        table_casestudy_batchid = ""
        table_parent_review_mapping = ""

        if payload['payload']['dimension_config']['nlp_type'].lower() == 'VoC'.lower():
            table_summary_table_prefix = config.GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX
            table_case_study_dimension_config = config.GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG
            table_nlp_output_case_study = config.GCP_BQ_TABLE_NLP_OUTPUT_CASE_STUDY
            table_polarity_trans = config.GCP_BQ_TABLE_POLARITY_TRANS
            table_language_trans = config.GCP_BQ_TABLE_LANGUAGE_TRANS
            table_case_study_company_source = config.GCP_BQ_TABLE_CASESTUDY_COMPANY_SOURCE
            table_batch_status = config.GCP_BQ_TABLE_BATCH_STATUS
            table_casestudy_batchid = config.GCP_BQ_TABLE_CASESTUDY_BATCHID
            table_parent_review_mapping = config.GCP_BQ_TABLE_PARENT_REVIEW_MAPPING
            table_review_country_mapping = config.GCP_BQ_TABLE_REVIEW_COUNTRY_MAPPING

        if payload['payload']['dimension_config']['nlp_type'].lower() == 'VoE'.lower():
            table_summary_table_prefix = config.GCP_BQ_TABLE_SUMMARY_TABLE_VOE_PREFIX
            table_case_study_dimension_config = config.GCP_BQ_TABLE_VOE_CASESTUDY_DIMENSION_CONFIG
            table_nlp_output_case_study = config.GCP_BQ_TABLE_VOE_NLP_OUTPUT_CASE_STUDY
            table_polarity_trans = config.GCP_BQ_TABLE_VOE_POLARITY_TRANS
            table_language_trans = config.GCP_BQ_TABLE_LANGUAGE_TRANS
            table_case_study_company_source = config.GCP_BQ_TABLE_VOE_CASESTUDY_COMPANY_SOURCE
            table_batch_status = config.GCP_BQ_TABLE_VOE_BATCH_STATUS
            table_casestudy_batchid = config.GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID
            table_parent_review_mapping = config.GCP_BQ_TABLE_VOE_PARENT_REVIEW_MAPPING
            table_review_country_mapping = config.GCP_BQ_TABLE_VOE_REVIEW_COUNTRY_MAPPING

        load_data_into_es_for_nlp_indexing(
            payload,
            case_study_id,
            run_id,
            table_summary_table_prefix,
            table_case_study_dimension_config,
            table_nlp_output_case_study,
            table_polarity_trans,
            table_language_trans,
            table_case_study_company_source,
            table_batch_status,
            table_casestudy_batchid,
            table_parent_review_mapping,
            table_review_country_mapping
        )

        event = "finish"
        error_text = ""
        logger.info(f"[MAIN] Load into ES completed successfully in {datetime.now() - start_time}!")
        
    except Exception as error:
        event = "fail"
        error_text = f'{error.__class__.__name__}: {error.args}'
        logger.exception(f"[MAIN] Error happens")
        logger.info(f"[MAIN] Elasticsearch nlp_index failed in {datetime.now() - start_time}!")
    
    finally:

        # Publishing to after_task

        # Sleep for a while to ensure that message is published sequently
        # This LOC for handle case message with progress lower is got later
        time.sleep(config.SLEEP)

        case_study_id = payload['case_study_id']
        run_id = payload['run_id']
        request_id = payload["request_id"]
        postprocess_object = payload['postprocess']
        payload = {
            "type": "nlp_index",
            "event": event,
            "postprocess": postprocess_object,
            "case_study_id": case_study_id,
            "request_id": request_id,
            "run_id": run_id,
            "error": error_text,
        }
        # return payload
        pubsub_util.publish(logger,
                            config.GCP_PROJECT_ID,
                            config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
                            payload)

if __name__ == "__main__":
    es_client = Elasticsearch(hosts=[config.ELASTICSEARCH_HOST])

    """
        subscription payload structure:
        {
            "type": "nlp_index",
            "case_study_id": 250,
            "request_id": 321,
            "postprocess": {},
            "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004"
        }

        after_task payload structure
        {
            "type": "nlp_index",
            "event": "finish",  // finish, fail
            "postprocess": {},
            "case_study_id": 250,
            "request_id": 321,
            "run_id": "b9f2602e-8bfb-11eb-beab-0242ac190004",
            "error": "",  // if the event is fail
        }

        BIG PAYLOAD
        {
            "type": "nlp_index",
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

    pubsub_util.subscribe(logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, handle_task)
