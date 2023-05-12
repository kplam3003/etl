"""VOC_5_add_topic_review_counts_column

Revision ID: c0b98cb515f6
Revises: 13b8210289d3
Create Date: 2021-01-16 09:13:47.934335

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import gcsfs
import pandas as pd
import ast
import time
import config


# revision identifiers, used by Alembic.
revision = 'c0b98cb515f6'
down_revision = '13b8210289d3'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

query_string1=f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_5_heatmapdim` AS

WITH tmp AS (SELECT 
    case_study_id,
    case_study_name,
    dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    source_name,
    company_name,
    company_id, 
    source_id,
    review_id,
    review_date,
    modified_dimension ,
    modified_label,
    split(terms,'	') AS single_terms,
    polarity,
    modified_polarity
    FROM `{project}.{dwh}.summary_table`
    WHERE dimension is not null    
    )
    
    ,count_topic AS    
    (SELECT 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension as dimension,
    modified_label as label,  
    polarity,
    count(distinct review_id) topic_review_counts,
    review_date as daily_date
    
    FROM `{project}.{dwh}.summary_table`
    WHERE dimension is not null
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,
    modified_label,        
    polarity,
    daily_date)
        
    ,single AS (SELECT  * EXCEPT(single_terms), single_terms FROM tmp, UNNEST(single_terms) AS single_terms)
        
    ,single2 AS (SELECT 
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    review_date as daily_date,
    modified_dimension as dimension,
    modified_label as label,
    single_terms,
    polarity,
    count(distinct review_id) as records,
    count(case when polarity != 'NONE' then review_id else null end) as sum_review_counts,
    sum( modified_polarity ) as sum_ss,
    count(distinct review_id) as collected_review_count

    FROM single a
    
    WHERE a.dimension_config_id is not null
 
    GROUP BY
    a.case_study_id,
    a.dimension_config_id,
    a.nlp_type,
    a.nlp_pack,
    a.company_id, 
    a.source_id,
    a.modified_dimension,
    a.modified_label,    
    single_terms,
    polarity,
    daily_date)
    select a.*, topic_review_counts
    
    FROM single2 a
    
    LEFT JOIN count_topic b
    on a.case_study_id = b.case_study_id
    and a.dimension_config_id = b.dimension_config_id
    and a.nlp_type =  b.nlp_type
    and a.nlp_pack = b.nlp_pack
    and a.company_id = b.company_id
    and a.source_id = b.source_id
    and a.dimension = b.dimension
    and a.label = b.label       
    and a.polarity = b.polarity
    and a.daily_date = b.daily_date;

# Create Meta Data Table in Bigquery:
    CREATE or REPLACE VIEW `{project}.{datamart}.dq_meta_tables` AS
    WITH ALL__TABLES__ AS (

    select * from `{project}`.{staging}.__TABLES__
    union all
    select * from `{project}`.{dwh}.__TABLES__
    union all
    select * from `{project}`.{datamart}.__TABLES__
    )

    SELECT a.table_catalog, 

    cast('{project}' as string) env,

    case when a.table_schema like '%datamart' then 'datamart' 
    when a.table_schema like '%dwh' then 'dwh' 
    when a.table_schema like '%staging' then 'staging'     
    else 'others' end database,

    a.table_schema ,a.table_name, a.creation_time, TIMESTAMP_MILLIS(c.last_modified_time) AS last_modified_time,  a.table_type, partition_column, c.row_count, round(c.size_bytes / POW(10,9),2) as GB, no_columns,  is_nullable, 
    is_int64,  is_float64, is_string, is_timestamp,list_columns,list_nullable, list_int64, list_float64, list_string, list_timestamp
    FROM (SELECT * FROM `region-US.INFORMATION_SCHEMA.TABLES`
    where table_schema in (SELECT distinct schema_name FROM `region-US.INFORMATION_SCHEMA.SCHEMATA`)) A
    LEFT JOIN 
    (SELECT table_catalog, table_schema , table_name, 

    max(case when is_partitioning_column = 'YES' then column_name else null end) AS partition_column,
    count(distinct column_name) no_columns, 
    STRING_AGG(column_name)  AS list_columns, 

    sum(case when is_nullable = 'YES' then 1 else 0 end) is_nullable, 
    STRING_AGG(case when is_nullable = 'YES' then column_name else null end)  AS list_nullable, 

    sum(case when data_type = 'INT64' then 1 else 0 end) is_int64, 
    STRING_AGG(case when data_type = 'INT64' then column_name else null end)  AS list_int64, 

    sum(case when data_type = 'FLOAT64' then 1 else 0 end) is_float64, 
    STRING_AGG(case when data_type = 'FLOAT64' then column_name else null end)  AS list_float64, 

    sum(case when data_type = 'STRING' then 1 else 0 end) is_string,
    STRING_AGG(case when data_type = 'STRING' then column_name else null end)  AS list_string, 

    sum(case when data_type = 'TIMESTAMP' then 1 else 0 end) is_timestamp,
    STRING_AGG(case when data_type = 'TIMESTAMP' then column_name else null end)  AS list_timestamp



    FROM `region-US.INFORMATION_SCHEMA.COLUMNS`

    group by table_catalog, table_schema , table_name) b

    on a.table_catalog = b.table_catalog
    and a.table_schema = b.table_schema
    and a.table_name = b.table_name

    LEFT JOIN ALL__TABLES__ C
    on a.table_catalog = c.project_id
    and a.table_schema = c.dataset_id
    and a.table_name = c.table_id;


"""

query_string2=f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_5_heatmapdim` 
    AS
    WITH tmp AS (SELECT 
    case_study_id,
    case_study_name,
    dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    source_name,
    company_name,
    company_id, 
    source_id,
    review_id,
    review_date,
    modified_dimension ,
    modified_label,
    split(terms,'\t') AS single_terms,
    polarity,
    modified_polarity
    FROM `{project}.{dwh}.summary_table`
    )
        
    ,single AS (SELECT  * EXCEPT(single_terms), single_terms FROM tmp, UNNEST(single_terms) AS single_terms)
        
    SELECT 
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,

    review_date as daily_date,
    modified_dimension as dimension,
    modified_label as label,
    single_terms,
    polarity,
    count(distinct review_id) as records,
    count(review_id) as sum_review_counts,
    sum( modified_polarity ) as sum_ss,
    count(distinct review_id) as collected_review_count

    FROM single
    WHERE dimension_config_id is not null
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,
    modified_label,    
    single_terms,
    polarity,
    daily_date
    ;
    
    DROP VIEW `{project}.{datamart}.dq_meta_tables`
    """