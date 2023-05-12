# standard library imports
import sys
sys.path.append('../')

import os
from datetime import datetime, timedelta

# external library imports
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound

# own library imports
import config
from utils import (
    make_postgres_connection,
    create_bigquery_client,
    convert_list_to_string,
    split_batch_name_into_company_source
)
from core.logger import init_logger

# create_logger
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def _calculate_group_lead_time(group, status_col_name, current_time):
    """
    Calculate lead time of groups in df.groupby operation.
    This mean lead time of requests, batches, steps
    """
    created_at_col = 'step_detail_created_at'
    updated_at_col = 'step_detail_updated_at'
        
    # for applying to groups
    if group[created_at_col].isnull().all():
        return np.nan
    elif group[status_col_name].isin(['running', 'waiting']).any():
        time_diff = current_time - group[created_at_col].min()
        time_diff_seconds = time_diff.seconds + time_diff.days * 24 * 60 * 60
        return time_diff_seconds
    else:
        time_diff = group[updated_at_col].max() - group[created_at_col].min()
        time_diff_seconds = time_diff.seconds + time_diff.days * 24 * 60 * 60
        return time_diff_seconds


def _calculate_step_detail_lead_time(row, status_col_name, current_time):
    """
    Calculate lead time of step details
    """
    created_at_col = 'step_detail_created_at'
    updated_at_col = 'step_detail_updated_at'
    
    # for applying to groups
    if pd.isnull(row[created_at_col]):
        return np.nan
    elif row[status_col_name] in (['running', 'waiting']):
        time_diff = current_time - row[created_at_col]
        time_diff_seconds = time_diff.seconds + time_diff.days * 24 * 60 * 60
        return time_diff_seconds
    else:
        time_diff = row[updated_at_col] - row[created_at_col]
        time_diff_seconds = time_diff.seconds + time_diff.days * 24 * 60 * 60
        return time_diff_seconds


def get_last_inserted_request_ids(bq_client, table_id):
    """
    Query and construct a dict of previously updated request ids.
    The dict has structure: {
        'latest': int, # latest added request id. Required.
        'running': [ints or empty] # running request ids. Required but can be an empty list.
    }
    """
    # standard query
    latest_updated_at_statement = f"""
        SELECT 
            case_study_id,
            request_id,
            request_status
        FROM `{table_id}` 
        WHERE inserted_at = (
            SELECT MAX(inserted_at)
            FROM `{table_id}` 
        )
        GROUP BY
            case_study_id,
            request_id,
            request_status
    """
    logger.info('[LEAD_TIME] Querying previously updated records...')
    try:
        # reload (return False) if table is empty
        table_object = bq_client.get_table(table_id)
        num_rows = table_object.num_rows
        if num_rows == 0:
            logger.warning(f'[LEAD_TIME] Table {table_id} is empty! Will reload.')
            return False
        
        # table not empty, query for last-updated case studies
        query_job = bq_client.query(latest_updated_at_statement)  # Make an API request.
        query_df = query_job.to_dataframe()
    except NotFound as e:
        # catch table not existed yet
        logger.exception(f'Table {table_id} not found!')
        return None
    if query_df.empty:
        # empty query
        logger.warning('[LEAD_TIME] Query returns no result')
        return None

    latest_id = query_df.request_id.max()
    running_ids = (query_df
                    [~query_df.request_status.isin(['finished', 'completed with error'])]
                    .request_id
                    .unique()
                    .tolist())
    logger.info(f'[LEAD_TIME] Latest request id so far: {latest_id}')
    logger.info(f'[LEAD_TIME] Currently running request ids: {running_ids}')
    request_ids_dict = {
        'latest': latest_id,
        'running': running_ids
    }
    return request_ids_dict


def query_full_step_detail_from_postgres(
        request_ids_dict: dict,
        max_connection_attempts: int = 3,
) -> pd.DataFrame:
    """
    Query full step detail from postgres for a given dict of request ids
    request_ids_dict: {"latest": int, "running": [ints or empty]}
    Will query full details of running requests, and requests > latest for new requests from etl system
    """
    # query step details from postgres
    cols = [
        'request_id', 'request_case_study_id', 'case_study_name', 'request_status', 'request_created_at', 'request_updated_at',
        'batch_id', 'batch_name', 'batch_status', 'batch_created_at', 'batch_updated_at',
        'step_id', 'step_name', 'step_type', 'step_status', 'step_created_at', 'step_updated_at',
        'step_detail_id', 'step_detail_name', 'step_detail_status', 'step_detail_created_at', 'step_detail_updated_at',
        'item_count',
    ]

    # data query
    if request_ids_dict is False:
        query = f"""
        SELECT *
        FROM mdm.etl_full_detail
        WHERE request_created_at > '{datetime.utcnow() - timedelta(days=config.RELOAD_TABLE_DAY_DELTA)}'
        """
    else:
        latest_request = request_ids_dict['latest']
        running_requests = request_ids_dict['running']
        query = f"""
        SELECT *
        FROM mdm.etl_full_detail
        WHERE request_id > {latest_request}
        """
        if running_requests:
            query += f"""OR request_id IN ({convert_list_to_string(running_requests)})"""

    # query for company id-name mapping
    company_map_query = """
    SELECT company_id, MIN(company_name) as company_name
    FROM mdm.etl_company_batch
    GROUP BY company_id
    ORDER BY company_id;
    """
    # query for source id-name mapping
    source_map_query = """
    SELECT source_id, MIN(source_name) as source_name
    FROM mdm.etl_company_batch
    WHERE source_name != ''
    GROUP BY source_id
    ORDER BY source_id;
    """
    
    # attempt to read from postgres database
    for attempt in range(1, max_connection_attempts + 1):
        logger.info(f'[LEAD_TIME] Connecting to PostgreSQL database, attempt number {attempt}...')
        _engine = make_postgres_connection(connection_uri=config.DATABASE_URI, connect_timeout=20)
        if not _engine:
            logger.critical('[LEAD_TIME] Cannot create engine for connection')
            raise ConnectionError('Cannot create engine for connection')
        try:
            with _engine.connect() as conn:
                # main query
                logger.info('[LEAD_TIME] Querying from table mdm.etl_full_detail...')
                df = pd.read_sql(query, conn)
                # company map query
                logger.info('[LEAD_TIME] Querying from table mdm.etl_company_batch for company mappings...')
                company_mapping_df = pd.read_sql(company_map_query, conn)
                # source map query
                logger.info('[LEAD_TIME] Querying from table mdm.etl_company_batch for source mappings...')
                source_mapping_df = pd.read_sql(source_map_query, conn)
                logger.info('[LEAD_TIME] Finished queries successfully')
            break
        except Exception as e:
            # pass exception silently
            logger.error(f'[LEAD_TIME] Error happens during execution. Error: {e.__class__.__name__}')
            logger.info(f'[LEAD_TIME] Error: {e}')
            continue
        finally:
            _engine.dispose()
    else:
        # pass silently if cannot connect to postgres after all attempts
        logger.error('Cannot connect to postgresql database...')
        return None
        
    # check query result empty
    if df.shape[0] != 0:
        logger.info(f"[LEAD_TIME] Shape of query result {df.shape}")
    else:
        logger.warning(f"Query results in 0 record. No preprocessing will be done")
        return None

    # process data
    logger.info('[LEAD_TIME] Pre-processing data...')
    lead_time_df = df[cols]
    lead_time_df = lead_time_df.rename({
        'request_case_study_id': 'case_study_id',
    }, axis=1)
    lead_time_df['step_detail_name'] = lead_time_df.step_detail_name.str.strip()

    # parse company id and source id
    logger.info('[LEAD_TIME] Parsing company_id and source_id from batch_name...')

    lead_time_df['batch_name'] = lead_time_df.batch_name.fillna('0-0')
    lead_time_df['company_id'], lead_time_df['source_id'] = zip(*lead_time_df
                                                                .batch_name
                                                                .apply(split_batch_name_into_company_source))
    lead_time_df['company_id'] = lead_time_df['company_id'].astype(int)
    lead_time_df['source_id'] = lead_time_df['source_id'].astype(int)
    # mapping
    logger.info('[LEAD_TIME] Mapping company_id to company_name...')
    lead_time_df = lead_time_df.merge(
        right=company_mapping_df,
        on='company_id',
        how='left'
    )
    logger.info('[LEAD_TIME] Mapping source_id to source_name...')
    lead_time_df = lead_time_df.merge(
        right=source_mapping_df,
        on='source_id',
        how='left'
    )
    logger.info('[LEAD_TIME] Finished preprocessing')
    lead_time_df = (lead_time_df
                    .sort_values(['request_id', 'request_status', 'request_created_at'])
                    .reset_index(drop=True)).replace('None', np.nan)
    
    logger.info(f'[LEAD_TIME] Dataframe columns: {list(lead_time_df.columns)}')
    return lead_time_df


def calculate_lead_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate lead time for each level: request, batch, step, step detail

    :param df: pd.DataFrame: dataframe on which to perform calculation
    :return: pd.DataFrame: original dataframe with lead time columns added
    """
    time_now = datetime.utcnow()
    logger.info('[LEAD_TIME] Calculating request lead time...')
    request_lead_time_df = (
        df
        .groupby(['case_study_id', 'request_id', 'request_status'])
        .apply(_calculate_group_lead_time, status_col_name='request_status', current_time=time_now)
        .reset_index()
        .rename({0: 'request_lead_time'}, axis=1)
    )
    logger.info('[LEAD_TIME] Calculating batch lead time...')
    batch_lead_time_df = (
        df
        .groupby(['request_id', 'request_status', 'batch_id', 'batch_name', 'batch_status'])
        .apply(_calculate_group_lead_time, status_col_name='batch_status', current_time=time_now)
        .reset_index()
        .rename({0: 'batch_lead_time'}, axis=1)
    )
    
    logger.info('[LEAD_TIME] Calculating step lead time...')
    step_lead_time_df = (
        df
        .groupby(['request_id', 'request_status', 'step_id', 'step_name', 'step_status'])
        .apply(_calculate_group_lead_time, status_col_name='step_status', current_time=time_now)
        .reset_index()
        .rename({0: 'step_lead_time'}, axis=1)
    )
    logger.info('[LEAD_TIME] Calculating step detail lead time...')
    step_detail_lead_time_series = df.apply(_calculate_step_detail_lead_time,
                                            status_col_name='step_detail_status',
                                            current_time=time_now,
                                            axis=1)
    step_detail_lead_time_df = df[[
        'step_detail_id', 'step_detail_name', 'step_detail_status'
    ]]
    step_detail_lead_time_df = (step_detail_lead_time_df
                                .assign(step_detail_lead_time=step_detail_lead_time_series.tolist()))

    # merge new info to original df
    added_columns_list = ('request_lead_time', 'batch_lead_time', 'step_lead_time', 'step_detail_lead_time')
    lead_time_df_list = (request_lead_time_df, batch_lead_time_df, step_lead_time_df, step_detail_lead_time_df)
    logger.info('[LEAD_TIME] Merging lead time to data...')
    
    result_df = df.copy()
    for i, _df in enumerate(lead_time_df_list):
        try:
            logger.info(f'[LEAD_TIME] Merge keys: {list(_df.columns)[:-1]}, new column: {list(_df.columns)[-1]}')
            result_df = result_df.merge(
                right=_df,
                on=list(_df.columns)[:-1],
                how='left'
            )
        except Exception as e:
            logger.exception(f'[LEAD_TIME] Error happens during merge for col {added_columns_list[i]}')
            result_df[added_columns_list[i]] = np.nan        
            
    result_df = result_df[[
        'case_study_id', 'case_study_name',
        'request_id', 'request_status',
        'request_created_at', 'request_updated_at', 'request_lead_time',
        'batch_id', 'batch_name', 'batch_status',
        'batch_created_at', 'batch_updated_at', 'batch_lead_time',
        'step_id', 'step_name', 'step_type', 'step_status',
        'step_created_at', 'step_updated_at', 'step_lead_time',
        'step_detail_id', 'step_detail_name', 'step_detail_status',
        'step_detail_created_at', 'step_detail_updated_at', 'step_detail_lead_time',
        'item_count',
        'company_id', 'company_name', 'source_id', 'source_name'
    ]]
    return result_df.drop_duplicates().reset_index(drop=True)


def load_lead_time_table_to_bigquery(
        df: pd.DataFrame,
        client: bq.Client,
        table_id: str,
        write_disposition="WRITE_TRUNCATE"
) -> bq.job.LoadJob:
    """
    Create a BigQuery load job and insert newly computed lead time df to respective table
    """
    
    logger.info('[LEAD_TIME] Setting up load job config...')
    table_schema = client.get_table(table_id).schema
    job_config = bq.LoadJobConfig(schema=table_schema,
                                  write_disposition=write_disposition,)
    logger.info('[LEAD_TIME] Creating load job...')
    df['inserted_at'] = datetime.utcnow()
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    logger.info('[LEAD_TIME] Executing load job...')
    job_start_time = datetime.utcnow()
    job.result()  # Wait for the job to complete.

    # info for load job
    logger.info(f'[LEAD_TIME] Load job finished, duration {(datetime.utcnow() - job_start_time).seconds} seconds')
    if job.errors is None:
        logger.info(f'[LEAD_TIME] Load job status: {job.state}')
    else:
        logger.error(f'[LEAD_TIME] Load job status: {job.state}')
        logger.error(f'[LEAD_TIME] Error happens when executing job. Error: {job.error_result}')
    logger.info(f'[LEAD_TIME] Bytes input to job: {job.input_file_bytes / 1024 ** 2:.4f} MB')
    logger.info(f'[LEAD_TIME] Bytes loaded by job: {job.output_bytes / 1024 ** 2:.4f} MB')
    logger.info(f'[LEAD_TIME] Rows loaded by job: {job.output_rows}')
    return job


def create_or_replace_dedup_table(client, base_table_id, dedup_table_id):
    """
    Run query to create the dedup table for dashboard. Will create if not exists, replace if exists
    """
    dedup_query_statement = f"""
    CREATE OR REPLACE TABLE {dedup_table_id}
    PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
    AS (
        WITH new_requests AS (
            SELECT
                request_id,
                MAX(inserted_at) as inserted_at
            FROM `{base_table_id}`
            GROUP BY 
                request_id
        )

        SELECT 
            L.*
        FROM `{base_table_id}` L
        INNER JOIN new_requests R
            ON L.request_id = R.request_id
            AND L.inserted_at = R.inserted_at
    )
    """
    logger.info('[LEAD_TIME] Creating or replacing dedup table...')
    try:
        query_job = client.query(dedup_query_statement)  # Make an API request.
        query_job.result()
    except Exception as e:
        # catch errors
        logger.error(f'[LEAD_TIME] Some error happened: {e}')
        raise e
    
    logger.info('[LEAD_TIME] Created or replaced dedup table successfully')
    logger.info(f'[LEAD_TIME] Bytes processed by query: {query_job.total_bytes_processed / 1024 ** 2:.4f} MB')
    logger.info(f'[LEAD_TIME] Bytes billed by query: {query_job.total_bytes_billed / 1024 ** 2:.4f} MB')
    return True


def run():
    # declare constants
    lead_time_table_id = config.GCP_BQ_TABLE_LEAD_TIME
    lead_time_dedup_mv_id = config.GCP_BQ_MV_LEAD_TIME_DEDUP
    
    # start script
    time_start = datetime.utcnow()
    # set up proper path
    logger.info(f"[LEAD_TIME] Starting {os.path.basename(__file__)}...")
    
    # set up bigquery client
    client = create_bigquery_client()
    
    # time from which to query for updates
    if config.RELOAD_LEAD_TIME_TABLE:
        logger.warning('[LEAD_TIME] Reloading lead time table...')
        request_ids_dict = False
        bq_write_disposition = 'WRITE_TRUNCATE'
    else:
        request_ids_dict = get_last_inserted_request_ids(bq_client=client,
                                                         table_id=lead_time_dedup_mv_id)
        bq_write_disposition = 'WRITE_APPEND'
        
    # exit if no new update found
    if request_ids_dict is None:
        logger.info('[LEAD_TIME] Exiting...')
        return 0

    # query full details of running requests and also new requests
    new_step_detail_df = query_full_step_detail_from_postgres(
        request_ids_dict=request_ids_dict,
        max_connection_attempts=3
    )
    # exit if nothing new since last update
    if new_step_detail_df is None:
        logger.info(f"[LEAD_TIME] Nothing new since last update. Exiting...")
        return 0
    logger.info(f"[LEAD_TIME] Number of new rows to be updated: {new_step_detail_df.shape[0]}")
    lead_time_df = calculate_lead_time(new_step_detail_df)
    
    # insert to bigquery
    load_lead_time_table_to_bigquery(client=client,
                                     df=lead_time_df,
                                     table_id=lead_time_table_id,
                                     write_disposition=bq_write_disposition)
        
    logger.info(f"[LEAD_TIME] {os.path.basename(__file__)} finished. Elapsed time: {(datetime.utcnow() - time_start)}")
    return 1
    
    

if __name__ == "__main__":
    run()