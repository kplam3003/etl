# standard library imports
import sys
sys.path.append('../')

import os
import json
from datetime import datetime, timedelta

# external library imports
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound
import gcsfs

# own library imports
import config
from utils import (
    create_bigquery_client,
    convert_list_to_string,
    split_batch_name_into_company_source
)
from core.logger import init_logger

# create_logger
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def _div(x, y):
    return 0 if y == 0 else x / y


def get_new_batches(bq_client, 
                    oldest_date, 
                    lead_time_dedup_table_id, 
                    batch_review_table_id) -> dict:
    """
    Get envs, batch ids from etl_lead_time that not yet exists in dq_check_batch_review
    """
    if config.TESTING:
        # testing
        batch_ids = [356, 357, 358, 359][-2:]
        batch_id_name_map = {
            356: '53-1-20210311',
            357: '53-2-20210311',
            358: '53-5-20210311',
            359: '56-5-20210311'
        }
        batch_cs_map = {
            356: 85,
            357: 85,
            358: 85,
            359: 85
        }
        batch_request_map = {
            356: 34,
            357: 34,
            358: 34,
            359: 34
        }
        return batch_ids, batch_id_name_map, batch_cs_map, batch_request_map
        
    # normal operation starts here
    query_statement = f"""
        WITH available_batches AS (
            SELECT 
                case_study_id,
                request_id,
                batch_id,
                batch_name,
                company_name,
                source_name,
                CASE 
                WHEN  lower(step_detail_name) like '%voc%' THEN "voc"
                WHEN lower(step_detail_name) like '%job%' THEN "voe_job"
                WHEN  lower(step_detail_name) like '%review%' THEN "voe"
                WHEN lower(step_detail_name) like '%overview%' THEN "voe_overview"
                ELSE NULL 
                END AS progress_type
            FROM `{lead_time_dedup_table_id}`
            WHERE batch_status in ('finished', 'completed with error')
                AND request_created_at >= '{oldest_date}'
                AND lower(step_detail_name) not like '%overview%'
            GROUP BY
                case_study_id,
                request_id,
                batch_id,
                batch_name,
                company_name,
                source_name,
                progress_type
        )

        SELECT
            L.case_study_id,
            L.request_id,
            L.batch_id,
            L.batch_name,
            L.company_name,
            L.source_name,
            L.progress_type
        FROM `available_batches` L
        LEFT JOIN `{batch_review_table_id}` R
        ON L.case_study_id = R.case_study_id
            AND L.request_id = R.request_id
            AND L.batch_id = R.batch_id
            AND L.batch_name = R.batch_name
        WHERE R.inserted_at is NULL
        GROUP BY
            L.case_study_id,
            L.request_id,
            L.batch_id,
            L.batch_name,
            L.company_name,
            L.source_name,
            L.progress_type
        ORDER BY
            L.request_id,
            L.batch_id
        ;
    """
    logger.info(f'[BATCH_REVIEW] Querying for newly completed batches...')
    try:
        query_job = bq_client.query(query_statement)
        batches_info = query_job.to_dataframe()
        if batches_info.shape[0] == 0:
            logger.info(f'[BATCH_REVIEW] No newly completed batches found')
            return None, None, None, None, None, None
    except NotFound as e:
        # if table not found, use a set of initial case study to create table
        logger.exception(f'[BATCH_REVIEW] Cannot query for newly completed batches: table not found.')
        return None, None, None, None, None, None
        
    
    logger.info(f'[BATCH_REVIEW] Number of new batches found: {batches_info.shape[0]}')
    # [batch_ids]
    batch_ids = batches_info.batch_id.unique().tolist()
    # {batch_id: batch_name}
    batch_id_name_map = dict(zip(batches_info.batch_id.tolist(), batches_info.batch_name.tolist()))
    # {batch_id: case_study_id}
    batch_cs_map = dict(zip(batches_info.batch_id.tolist(), batches_info.case_study_id.tolist()))
    # {batch_id: request_id}
    batch_request_map = dict(zip(batches_info.batch_id.tolist(), batches_info.request_id.tolist()))        
    # {batch_id: source_name}
    batch_source_map = dict(zip(batches_info.batch_id.tolist(), batches_info.source_name.tolist())) 
    # {batch_id: progress_type}
    batch_progress_type_map = dict(zip(batches_info.batch_id.tolist(), batches_info.progress_type.tolist())) 
    return batch_ids, batch_id_name_map, batch_cs_map, batch_request_map, batch_source_map, batch_progress_type_map


def fetch_lead_time(bq_client, batch_ids, lead_time_dedup_table_id):
    # extract information about batch on Postgre
    # set up batch ids:
    batch_ids_string = convert_list_to_string(batch_ids)
    
    query_string = f"""
    SELECT 
        request_id,
        request_status,
        case_study_id,
        case_study_name,
        batch_id,
        batch_name,
        batch_status,
        company_id,
        company_name,
        source_id,
        source_name,
        step_type,
        step_status,
        SUM(item_count) as total_rows,
        MAX(step_lead_time) as step_lead_time
    FROM `{lead_time_dedup_table_id}`
    WHERE batch_id in ({batch_ids_string})
        AND batch_status in ('finished', 'completed with error')
        AND lower(step_detail_name) not like '%overview%'
    GROUP BY
        request_id,
        request_status,
        case_study_id,
        case_study_name,
        batch_id,
        batch_name,
        batch_status,
        company_id,
        company_name,
        source_id,
        source_name,
        step_type,
        step_status
    """
    logger.info(f'[BATCH_REVIEW] Querying for lead time info...')
    logger.info(f'[BATCH_REVIEW] Number of batches: {len(batch_ids)}')
    lead_time_df = bq_client.query(query_string).to_dataframe()
    return lead_time_df


def fetch_voc_load_results(bq_client, load_table_id, batch_ids):
    # extract the reviews that are loaded into bigquery by ETL
    batch_ids_string = convert_list_to_string(batch_ids)
    query_string = f"""
        SELECT
            case_study_id,
            request_id,
            batch_id,
            batch_name,
            COUNT(review_id) as num_rows_in_files,
            COUNT(DISTINCT(review_id)) as num_unique_reviews,
            COUNT(DISTINCT(file_name)) as num_files,
        FROM `{load_table_id}`
        WHERE batch_id in ({batch_ids_string})
        GROUP BY
            case_study_id,
            request_id,
            batch_id,
            batch_name
    """
    logger.info(f'[BATCH_REVIEW] Querying ETL load results...')
    query_job = bq_client.query(query_string)
    load_result_df = query_job.to_dataframe()
    if load_result_df.empty:
        logger.warning(f'[BATCH_REVIEW] Queried results has 0 rows')
    else:
        logger.info(f'[BATCH_REVIEW] Queried results has {load_result_df.shape[0]} rows/batch_ids')
    load_result_df['num_output'] = load_result_df.num_unique_reviews
    load_result_df['step_type'] = 'load'
    return load_result_df

def fetch_voe_load_results(bq_client, load_table_id, batch_ids):
    # extract the reviews that are loaded into bigquery by ETL
    batch_ids_string = convert_list_to_string(batch_ids)
    query_string = f"""
        SELECT
            case_study_id,
            request_id,
            batch_id,
            batch_name,
            COUNT(review_id) as num_rows_in_files,
            COUNT(DISTINCT(review_id)) as num_unique_reviews,
            COUNT(DISTINCT(file_name)) as num_files,
        FROM `{load_table_id}`
        WHERE batch_id in ({batch_ids_string})
        GROUP BY
            case_study_id,
            request_id,
            batch_id,
            batch_name
    """
    logger.info(f'[BATCH_REVIEW] Querying ETL load results...')
    query_job = bq_client.query(query_string)
    load_result_df = query_job.to_dataframe()
    if load_result_df.empty:
        logger.warning(f'[BATCH_REVIEW] Queried results has 0 rows')
    else:
        logger.info(f'[BATCH_REVIEW] Queried results has {load_result_df.shape[0]} rows/batch_ids')
    load_result_df['num_output'] = load_result_df.num_unique_reviews
    load_result_df['step_type'] = 'load'
    return load_result_df

def fetch_voe_job_load_results(bq_client, load_table_id, batch_ids):
    # extract the reviews that are loaded into bigquery by ETL
    batch_ids_string = convert_list_to_string(batch_ids)
    query_string = f"""
        SELECT
            case_study_id,
            request_id,
            batch_id,
            batch_name,
            COUNT(job_id) as num_rows_in_files,
            COUNT(DISTINCT(job_id)) as num_unique_reviews,
            COUNT(DISTINCT(file_name)) as num_files,
        FROM `{load_table_id}`
        WHERE batch_id in ({batch_ids_string})
        GROUP BY
            case_study_id,
            request_id,
            batch_id,
            batch_name
    """
    logger.info(f'[BATCH_REVIEW] Querying ETL load results...')
    query_job = bq_client.query(query_string)
    load_result_df = query_job.to_dataframe()
    if load_result_df.empty:
        logger.warning(f'[BATCH_REVIEW] Queried results has 0 rows')
    else:
        logger.info(f'[BATCH_REVIEW] Queried results has {load_result_df.shape[0]} rows/batch_ids')
    load_result_df['num_output'] = load_result_df.num_unique_reviews
    load_result_df['step_type'] = 'load'
    return load_result_df

def fetch_load_results(bq_client, batch_progress_type_map,voc_table_id, voe_table_id,voe_job_table_id):
    # extract the reviews that are loaded into bigquery by ETL

    load_results_df = list()
    for progress_type in set(batch_progress_type_map.values()):
        if progress_type == "voc":
            batch_ids = [batch_id for batch_id, progress_type in batch_progress_type_map.items() if progress_type == 'voc']
            load_df = fetch_voc_load_results(bq_client = bq_client, load_table_id= voc_table_id , batch_ids= batch_ids)
        elif progress_type == "voe":
            batch_ids = [batch_id for batch_id, progress_type in batch_progress_type_map.items() if progress_type == 'voe']
            load_df = fetch_voe_load_results(bq_client = bq_client, load_table_id= voe_table_id , batch_ids= batch_ids)
        elif progress_type == "voe_job":
            batch_ids = [batch_id for batch_id, progress_type in batch_progress_type_map.items() if progress_type == 'voe_job']
            load_df = fetch_voe_job_load_results(bq_client = bq_client, load_table_id= voe_job_table_id , batch_ids= batch_ids)
        else:
            load_df = pd.DataFrame()
        load_results_df.append(load_df)
    load_result_df=pd.concat(load_results_df , ignore_index=True)

    if load_result_df.empty:
        logger.warning(f'[BATCH_REVIEW] Queried results has 0 rows')
    else:
        logger.info(f'[BATCH_REVIEW] Queried results has {load_result_df.shape[0]} rows/batch_ids')
    
    return load_result_df



def _process_voc_batch_df_from_gcs(df, batch_id, batch_name, source_name,step, num_input_reviews):
    if df.empty:
        row_dict = {
            'batch_id': batch_id,
            'batch_name': batch_name,
            'step_type': step,
            'num_files': np.nan,
            'num_rows_in_files': np.nan,
            'num_unique_reviews': np.nan,
            'num_input': np.nan,
            'num_output': np.nan,
            'num_empty_nlp_response': np.nan,
            'success_percent': np.nan,
        }
        return row_dict
        
    has_id_col = False
    num_unique_reviews = df.shape[0]
    
    for col in df.columns[::-1]:
        if col in ('review_id', 'reviewId'):
            has_id_col = True
            review_id_col = col
            num_unique_reviews = df[col].nunique()
            break
        
    if not has_id_col:
        logger.warning(f'[BATCH_REVIEW] Batch {batch_name} has no review id column, cannot calculate number of unique reviews')
        
    num_rows_in_files = df.shape[0]
    num_files = df.filename.nunique()
    row_dict = {
        'batch_id': batch_id,
        'batch_name': batch_name,
        'step_type': step,
        'num_files': num_files,
        'num_rows_in_files': num_rows_in_files,
        'num_unique_reviews': num_unique_reviews
    }
    
    num_empty_nlp_response = np.nan
    # calculate percent success
    # crawl is reference step
    if step == 'crawl':
        num_input_reviews = num_unique_reviews
        num_output_reviews = num_unique_reviews
    
    # preprocess takes reviews from crawl and filter
    if step == 'preprocess':
        num_output_reviews = num_unique_reviews
        # for Capterra case
        if 'capterra'in source_name.lower():
            num_input_reviews = num_input_reviews * 3

    # translate takes from preprocess and send to API for results
    if step == 'translate':
        if has_id_col:
            num_output_reviews = (df
                                  [df['trans_status'] == True]
                                  [review_id_col]
                                  .nunique())
        else:
            num_output_reviews = df.loc[df['trans_status'] == True].shape[0]
    
    # nlp takes from translate and send to API for result
    if step == 'nlp':
        invalid_df = df[
            (df['trans_status'] == True) 
            & 
            (
                (df['nlp_transformed'] == '[]') 
                | 
                df['nlp_transformed'].isnull()
                |
                (df['nlp_transformed'] == 'nan')
            )
        ]
        empty_response_df = df[
            (df['trans_status'] == True) 
            &
            (df['nlp_transformed'] == '[]') 
        ]
        if has_id_col:
            num_invalid_reviews = invalid_df[review_id_col].nunique()
            num_empty_nlp_response = empty_response_df[review_id_col].nunique()
        else:
            num_invalid_reviews = invalid_df.shape[0]
            num_empty_nlp_response = empty_response_df.shape[0]
        num_output_reviews = num_input_reviews - num_invalid_reviews
        
    # calculate success percentage
    if num_input_reviews == 0:
        success_percent = np.nan
    else:
        success_percent = num_output_reviews / num_input_reviews * 100
    success_dict = {
        'num_input': num_input_reviews,
        'num_output': num_output_reviews,
        'num_empty_nlp_response': num_empty_nlp_response,
        'success_percent': success_percent,
    }
        
    # merge 2 dict:
    row_dict.update(success_dict)
    
    logger.info(f'[BATCH_REVIEW] Resulting step info: {json.dumps(row_dict)}')
    
    return row_dict

def _process_voe_batch_df_from_gcs(df, batch_id, batch_name, source_name,step, num_input_reviews):
    if df.empty:
        row_dict = {
            'batch_id': batch_id,
            'batch_name': batch_name,
            'step_type': step,
            'num_files': np.nan,
            'num_rows_in_files': np.nan,
            'num_unique_reviews': np.nan,
            'num_input': np.nan,
            'num_output': np.nan,
            'num_empty_nlp_response': np.nan,
            'success_percent': np.nan,
        }
        return row_dict
        
    has_id_col = False
    num_unique_reviews = df.shape[0]
    
    for col in df.columns[::-1]:
        if col in ('review_id', 'reviewId'):
            has_id_col = True
            review_id_col = col
            num_unique_reviews = df[col].nunique()
            break
        
    if not has_id_col:
        logger.warning(f'[BATCH_REVIEW] Batch {batch_name} has no review id column, cannot calculate number of unique reviews')
        
    num_rows_in_files = df.shape[0]
    num_files = df.filename.nunique()
    row_dict = {
        'batch_id': batch_id,
        'batch_name': batch_name,
        'step_type': step,
        'num_files': num_files,
        'num_rows_in_files': num_rows_in_files,
        'num_unique_reviews': num_unique_reviews
    }
    
    num_empty_nlp_response = np.nan
    # calculate percent success
    # crawl is reference step
    if step == 'crawl':
        num_input_reviews = num_unique_reviews
        num_output_reviews = num_unique_reviews
    
    # preprocess takes reviews from crawl and filter
    if step == 'preprocess':
        num_output_reviews = num_unique_reviews
        # for Indeed case
        if "indeed" in source_name.lower():
            num_input_reviews = num_input_reviews * 3
        # for Glassdoor case
        else: 
            num_input_reviews = num_input_reviews * 2


    # translate takes from preprocess and send to API for results
    if step == 'translate':
        if has_id_col:
            num_output_reviews = (df
                                  [df['trans_status_review'] == True]
                                  [review_id_col]
                                  .nunique())
        else:
            num_output_reviews = df.loc[df['trans_status_review'] == True].shape[0]
    
    # nlp takes from translate and send to API for result
    if step == 'nlp':
        invalid_df = df[
            (df['trans_status_review'] == True) 
            & 
            (
                (df['nlp_transformed_review'] == '[]') 
                | 
                df['nlp_transformed_review'].isnull()
                |
                (df['nlp_transformed_review'] == 'nan')
            )
        ]
        empty_response_df = df[
            (df['trans_status_review'] == True) 
            &
            (df['nlp_transformed_review'] == '[]') 
        ]
        if has_id_col:
            num_invalid_reviews = invalid_df[review_id_col].nunique()
            num_empty_nlp_response = empty_response_df[review_id_col].nunique()
        else:
            num_invalid_reviews = invalid_df.shape[0]
            num_empty_nlp_response = empty_response_df.shape[0]
        num_output_reviews = num_input_reviews - num_invalid_reviews
        
    # calculate success percentage
    if num_input_reviews == 0:
        success_percent = np.nan
    else:
        success_percent = num_output_reviews / num_input_reviews * 100
    success_dict = {
        'num_input': num_input_reviews,
        'num_output': num_output_reviews,
        'num_empty_nlp_response': num_empty_nlp_response,
        'success_percent': success_percent,
    }
        
    # merge 2 dict:
    row_dict.update(success_dict)
    logger.info(f'[BATCH_REVIEW] Resulting step info: {json.dumps(row_dict)}')
    return row_dict


def _process_job_batch_df_from_gcs(df, batch_id, batch_name,step, num_input_reviews):
    if df.empty:
        row_dict = {
            'batch_id': batch_id,
            'batch_name': batch_name,
            'step_type': step,
            'num_files': np.nan,
            'num_rows_in_files': np.nan,
            'num_unique_reviews': np.nan,
            'num_input': np.nan,
            'num_output': np.nan,
            'num_empty_nlp_response': np.nan,
            'success_percent': np.nan,
        }
        return row_dict
        
    has_id_col = False
    num_unique_reviews = df.shape[0]
    
    for col in df.columns:
        if col in ('job_id'):
            has_id_col = True
            job_col = col
            num_unique_reviews = df[col].nunique()
            break
        
    if not has_id_col:
        logger.warning(f'[BATCH_REVIEW] Batch {batch_name} has no review id column, cannot calculate number of unique reviews')
        
    num_rows_in_files = df.shape[0]
    num_files = df.filename.nunique()
    row_dict = {
        'batch_id': batch_id,
        'batch_name': batch_name,
        'step_type': step,
        'num_files': num_files,
        'num_rows_in_files': num_rows_in_files,
        'num_unique_reviews': num_unique_reviews
    }
    
    num_empty_nlp_response = np.nan
    # calculate percent success
    # crawl is reference step
    if step == 'crawl':
        num_input_reviews = num_unique_reviews
        num_output_reviews = num_unique_reviews
    
    # preprocess takes reviews from crawl and filter
    if step == 'preprocess':
        num_output_reviews = num_unique_reviews

    # translate takes from preprocess and send to API for results
    if step == 'translate':
        if has_id_col:
            num_output_reviews = (df[job_col]
                                  .nunique())
        else:
            num_output_reviews = df.shape[0]
    
    # nlp takes from translate and send to API for result
    if step == 'nlp':
        if has_id_col:
            num_output_reviews=(df[job_col]
                                  .nunique())
           
        else:
            num_output_reviews = df.shape[0]
        
    # calculate success percentage
    if num_input_reviews == 0:
        success_percent = np.nan
    else:
        success_percent = num_output_reviews / num_input_reviews * 100
    success_dict = {
        'num_input': num_input_reviews,
        'num_output': num_output_reviews,
        'num_empty_nlp_response': num_empty_nlp_response,
        'success_percent': success_percent,
    }
        
    # merge 2 dict:
    row_dict.update(success_dict)
    logger.info(f'[BATCH_REVIEW] Resulting step info: {json.dumps(row_dict)}')
    return row_dict



def _load_batch_from_gcs(project, bucket, batch_id, batch_name,source_name, progress_type):
    # all steps, excluding load
    steps = ['crawl', 'preprocess', 'translate', 'nlp']
    bucket_fs = gcsfs.GCSFileSystem(project=project)
    
    logger.info(f'[BATCH_REVIEW] Loading batch id: {batch_id}, name: {batch_name}')
    num_review_from_previous_step = None
    batch_all_steps_dicts = []
    # navigate to each step and download the needed files from given batch
    for step in steps:
        source_path = f'{bucket}/{step}/{batch_name}/*'
        logger.info(f'[BATCH_REVIEW] GCS batch path: gs://{source_path}')
        file_paths = bucket_fs.glob(source_path)
        
        if len(file_paths) == 0:
            logger.warning(f'[BATCH_REVIEW] {source_path} has no file or does not exist')
            raw_df = pd.DataFrame()
        else:
            # try to read csv from gcs using pandas via gcsfs
            logger.info(f'[BATCH_REVIEW] Number of files in {step}: {len(file_paths)}')
            non_empty_file_paths = [f for f in file_paths if bucket_fs.size(f) > 2]
            logger.info(f'[BATCH_REVIEW] Number of non-empty files: {len(non_empty_file_paths)}')
            files_df = []
            for file in non_empty_file_paths:
                logger.info(f'[BATCH_REVIEW] Reading file: {file}')
                _read_path = f'gs://{file}'
                try:
                    _df = pd.read_csv(_read_path, sep='\t', parse_dates=True)
                    _df['filename'] = file.split('/')[-1]
                    files_df.append(_df)
                except Exception as e: # empty file
                    logger.exception(f'[BATCH_REVIEW] Error while reading: {e.__class__.__name__}')
                    continue
                
            if files_df:
                raw_df = pd.concat(files_df)
            else:
                raw_df = pd.DataFrame()
        if progress_type == "voc":
            _processed_batch_info = _process_voc_batch_df_from_gcs(df=raw_df,
                                                                batch_id=batch_id,
                                                                batch_name=batch_name,
                                                                source_name=source_name,
                                                                step=step,
                                                                num_input_reviews=num_review_from_previous_step)
            num_review_from_previous_step = _processed_batch_info.get('num_output')
        elif progress_type == "voe":
            _processed_batch_info = _process_voe_batch_df_from_gcs(df=raw_df,
                                                                batch_id=batch_id,
                                                                batch_name=batch_name,
                                                                source_name=source_name,
                                                                step=step,
                                                                num_input_reviews=num_review_from_previous_step)
            num_review_from_previous_step = _processed_batch_info.get('num_output')
        elif  progress_type == "voe_job":
            _processed_batch_info = _process_job_batch_df_from_gcs(df=raw_df,
                                                                batch_id=batch_id,
                                                                batch_name=batch_name,
                                                                step=step,
                                                                num_input_reviews=num_review_from_previous_step)
            num_review_from_previous_step = _processed_batch_info.get('num_output')
        else: 
            _processed_batch_info = {
                'batch_id': batch_id,
                'batch_name': batch_name,
                'step_type': step,
            }
            
        batch_all_steps_dicts.append(_processed_batch_info)  
    
    return batch_all_steps_dicts
    
    
def load_batches_from_gcs(project, bucket, batch_ids, batch_id_name_map,batch_source_map,batch_progress_type_map ):
    
    logger.info(f'[BATCH_REVIEW] Loading batches from GCS...')
    batch_dicts = []
    num_batch_ids = len(batch_ids)
    for i, batch_id in enumerate(batch_ids):
        try:
            batch_name = batch_id_name_map[batch_id]
            source_name = batch_source_map[batch_id]
            progress_type = batch_progress_type_map[batch_id]
            logger.info(f'[BATCH_REVIEW] Loading batch {i + 1}/{num_batch_ids} from GCS to dataframe')
            _batch_all_steps_dicts = _load_batch_from_gcs(project=project, 
                                                          bucket=bucket, 
                                                          batch_id=batch_id, 
                                                          batch_name=batch_name,
                                                          source_name=source_name,
                                                          progress_type=progress_type
                                                          )
            batch_dicts.extend(_batch_all_steps_dicts)
        except Exception as error:
            logger.exception(f'[BATCH_REVIEW] Error hapens while loading batch from GCS!')
    
    batch_info_df = pd.DataFrame(batch_dicts)
    return batch_info_df


def construct_final_table(lead_time_df, batch_info_df, load_results_df, batch_cs_map, batch_request_map):
    
    def _get_id_from_map(df, mapping_dict):
        """
        Given a mapping dict of format {batch_id: case_study_id/request_id}
        """
        return mapping_dict.get(df['batch_id'], np.nan)
    
    logger.info(f'[BATCH_REVIEW] Constructing final table...')
    # map case study to data from GCS storage
    batch_info_df['case_study_id'] = (batch_info_df
                                      [['batch_id']]
                                      .apply(_get_id_from_map, 
                                             mapping_dict=batch_cs_map, 
                                             axis=1))
    batch_info_df['request_id'] = (batch_info_df
                                   [['batch_id']]
                                   .apply(_get_id_from_map, 
                                          mapping_dict=batch_request_map, 
                                          axis=1))
    """
    - process and create final table to insert into bigquery
    - add 3 process result columns to load_results
    - concat load step into batch info
    """
    
    batch_info_full_df = pd.concat([batch_info_df, load_results_df])
    
    batch_info_full_df = (
        batch_info_full_df
        .sort_values(['case_study_id', 'request_id', 'batch_id', 'batch_name', 'step_type'])
        .reset_index(drop=True)
    )
    
    # add lead time to full batch info
    batch_info_with_time_df = lead_time_df.merge(
        right=batch_info_full_df,
        how='left',
        on=['case_study_id', 'request_id', 'batch_id', 'step_type', 'batch_name'],
    )
    # calculate rows per second
    batch_info_with_time_df['rows_per_second'] = (batch_info_with_time_df
                                                  .apply(lambda row : _div(row['num_rows_in_files'], row['step_lead_time']), axis=1))
    
    # add num_input for load step as num_output of preprocess step
    load_num_input = (batch_info_with_time_df
                      .loc[batch_info_with_time_df.step_type == 'preprocess', 'num_output']
                      .tolist())
    num_load_steps = batch_info_with_time_df.loc[batch_info_with_time_df.step_type == 'load', 'num_input'].shape[0]
    logger.info(f'[BATCH_REVIEW] Number of batches with preprocess: {len(load_num_input)}')
    logger.info(f"[BATCH_REVIEW] Number of batches with load: {num_load_steps}")

    groups = batch_info_with_time_df.groupby(['batch_id', 'batch_name'])
    for name, group in groups:
        _id = name[0]
        _name = name[1]
        steps = group.step_type.unique()
        has_load = 'load' in steps
        has_preprocess = 'preprocess' in steps
        if has_load and has_preprocess:
            # load input is the output of preprocess step
            logger.info(f'[BATCH_REVIEW] Batch {_id}, name {_name} has both load and preprocess steps')
            _num_load_output = group.loc[(group.step_type == 'load'), 'num_output'].iloc[0]
            _num_load_input = group.loc[(group.step_type == 'preprocess'), 'num_output'].iloc[0]
            
            batch_info_with_time_df.loc[
                (batch_info_with_time_df.batch_id == _id)
                &
                (batch_info_with_time_df.step_type == 'load'),
                'num_input'
            ] = _num_load_input
            
            batch_info_with_time_df.loc[
                (batch_info_with_time_df.batch_id == _id)
                &
                (batch_info_with_time_df.step_type == 'load'),
                'success_percent'
            ] = _num_load_output / _num_load_input * 100
             
        elif has_load and not has_preprocess:
            # sometimes a batch may have no files on GCS but has data loaded on BQ
            # maybe because someone deleted the batches
            logger.warning(f'[BATCH_REVIEW] Batch {_id}, name {_name} has load but not preprocess step!')
            batch_info_with_time_df.loc[
                (batch_info_with_time_df.batch_id == _id)
                &
                (batch_info_with_time_df.step_type == 'load'),
                'num_input'
            ] = np.nan
            batch_info_with_time_df.loc[
                (batch_info_with_time_df.batch_id == _id)
                &
                (batch_info_with_time_df.step_type == 'load'),
                'success_percent'
            ] = np.nan
        elif not has_load and not has_preprocess:
            logger.warning(f'[BATCH_REVIEW] Batch {_id}, name {_name} has neither preprocess nor load step')
        else:
            logger.warning(f'[BATCH_REVIEW] Batch {_id}, name {_name} has preprocess but not load step')
    
    # rearrage and some minor preprocessing
    final_df = (batch_info_with_time_df
        [['request_id', 'request_status',
          'case_study_id', 'case_study_name',
          'batch_id', 'batch_name', 'batch_status',
          'company_id', 'company_name', 'source_id', 'source_name',
          'step_type', 'step_status', 'step_lead_time', 'num_files', 
          'total_rows', 'num_rows_in_files', 'num_unique_reviews', 'rows_per_second',
          'num_input', 'num_output', 'num_empty_nlp_response', 'success_percent']])
    final_df['total_rows'] = final_df['total_rows'].fillna(0)
    final_df['company_name'] = final_df['company_name'].fillna('Unknown')
    final_df['source_name'] = final_df['source_name'].fillna('Unknown')
    final_df['inserted_at'] = datetime.utcnow()
    return final_df


def insert_table_into_bigquery(bq_client, df, table_id):
    # get schema of current table
    table_schema = bq_client.get_table(table_id).schema
    
    logger.info(f'[BATCH_REVIEW] Loading to table {table_id}')
    table_fields = tuple([f.name for f in table_schema])
    logger.info(f'[BATCH_REVIEW] Table has {len(table_fields)} fields: {table_fields}')
    logger.info(f'[BATCH_REVIEW] Number of records to be loaded: {df.shape[0]} rows, {df.shape[1]} columns')
    logger.info(f'[BATCH_REVIEW] Setting up load job config...')
    job_config = bq.LoadJobConfig(
        schema=table_schema,
        write_disposition="WRITE_APPEND",
    )
    logger.info(f'[BATCH_REVIEW] Creating load job...')
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    logger.info(f'[BATCH_REVIEW] Executing load job...')
    job_start_time = datetime.utcnow()
    try:
        job.result()  # Wait for the job to complete.

        # info for load job
        logger.info(f'[BATCH_REVIEW] Load job finished, duration {(datetime.utcnow() - job_start_time).seconds} seconds')
        if job.errors is None:
            logger.info(f'[BATCH_REVIEW] Load job status: {job.state}')
        else:
            logger.error(f'[BATCH_REVIEW] Load job status: {job.state}')
            logger.error(f'[BATCH_REVIEW] Error happens when executing job. Error: {job.error_result}')
        logger.info(f'[BATCH_REVIEW] Bytes input to job: {job.input_file_bytes / 1024 ** 2:.4f} MB')
        logger.info(f'[BATCH_REVIEW] Bytes loaded by job: {job.output_bytes / 1024 ** 2:.4f} MB')
        logger.info(f'[BATCH_REVIEW] Rows loaded by job: {job.output_rows}')
        return
    except Exception as e:
        logger.exception(f'[BATCH_REVIEW] Error during load job execution!')
        return


def run():
    # declare constants
    lead_time_dedup_table_id = config.GCP_BQ_MV_LEAD_TIME_DEDUP
    batch_review_table_id = config.GCP_BQ_TABLE_BATCH_REVIEW 
    voc_table_id = config.GCP_BQ_TABLE_VOC
    voe_table_id = config.GCP_BQ_TABLE_VOE
    voe_job_table_id = config.GCP_BQ_TABLE_VOE_JOB
    # start script
    time_start = datetime.utcnow()
    # set up proper path
    logger.info(f"[BATCH_REVIEW] Starting {os.path.basename(__file__)}...")
    
    # set up bigquery client
    client = create_bigquery_client()
    
    # get necessary data from sources
    logger.info(f'[BATCH_REVIEW] Fetching necessary data...')
    # new batches and maps from BigQuery
    oldest_date = datetime.utcnow() - timedelta(days=config.RELOAD_TABLE_DAY_DELTA)
    batch_ids, batch_id_name_map, batch_cs_map, batch_request_map, batch_source_map, batch_progress_type_map = get_new_batches(client, 
                                                                                                                               oldest_date=oldest_date,
                                                                                                                               lead_time_dedup_table_id=lead_time_dedup_table_id,
                                                                                                                               batch_review_table_id=batch_review_table_id)
    if batch_ids is None:
        logger.info(f'[BATCH_REVIEW] Exiting...')
        return 0
    
    # limiting the number of batches, for testing purpose
    if config.BATCH_LIMIT is not None:
        batch_ids = batch_ids[:config.BATCH_LIMIT]
    
    # lead time from BigQuery
    lead_time_df = fetch_lead_time(bq_client=client, 
                                   batch_ids=batch_ids,
                                   lead_time_dedup_table_id=lead_time_dedup_table_id)
    # ETL load results from BigQuery
    load_results_df = fetch_load_results(bq_client=client,
                                         batch_progress_type_map=batch_progress_type_map,
                                         voc_table_id=voc_table_id,
                                         voe_table_id=voe_table_id,
                                         voe_job_table_id=voe_job_table_id
                                         )
    # files and review count/nunique from GCStorage
    batch_info_df = load_batches_from_gcs(project=config.GCP_PROJECT,
                                          bucket=config.GCP_STORAGE_BUCKET,
                                          batch_ids=batch_ids,
                                          batch_id_name_map=batch_id_name_map,
                                          batch_source_map=batch_source_map,
                                          batch_progress_type_map=batch_progress_type_map)
    
    batch_info_with_time_df = construct_final_table(lead_time_df=lead_time_df,
                                                    batch_info_df=batch_info_df,
                                                    load_results_df=load_results_df,
                                                    batch_cs_map=batch_cs_map,
                                                    batch_request_map=batch_request_map)

    logger.info(f'[BATCH_REVIEW] Loading to {batch_review_table_id}')
    
    insert_table_into_bigquery(df=batch_info_with_time_df,
                               table_id=batch_review_table_id,
                               bq_client=client)
    logger.info(f"[BATCH_REVIEW] {os.path.basename(__file__)} finished. Elapsed time: {(datetime.utcnow() - time_start)}")
    return 1


if __name__ == '__main__':
    # execute
    run()