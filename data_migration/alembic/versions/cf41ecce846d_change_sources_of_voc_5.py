"""change_sources_of_voc_5

Revision ID: cf41ecce846d
Revises: 916fa721e6b2
Create Date: 2021-02-01 05:14:54.220148

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
revision = 'cf41ecce846d'
down_revision = '916fa721e6b2'
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
    """change_sources_of_voc_5"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""
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
    split(terms,'	') AS single_terms,
    polarity,
    modified_polarity
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null   
    
    )

     ,single AS (SELECT  * EXCEPT(single_terms), single_terms FROM tmp, UNNEST(single_terms) AS single_terms)
        
    ,count_terms AS (SELECT 
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
    
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null
    # and case_study_id =  144
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

    ,count_dimension AS 

    (SELECT 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension as dimension,
    sum(CASE WHEN modified_polarity is null THEN 0 ELSE modified_polarity END) as sum_ss,
    count(CASE WHEN (polarity != 'NONE') AND (polarity is not null) THEN  review_id ELSE null END) as sum_review_counts,
    review_date as daily_date
    
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null
    # and case_study_id =  144
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,    
    daily_date)

    ,count_records AS 

    (SELECT 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    
    count(distinct review_id) as records,
    review_date as daily_date
    
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null
    
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,      
    daily_date)   
        
    select a.*
    , topic_review_counts, 
    sum_ss, sum_review_counts, records
    
    FROM count_terms a
    
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
    and date(a.daily_date) = date(b.daily_date)
    
    LEFT JOIN count_dimension c
    on a.case_study_id = c.case_study_id
    and a.dimension_config_id = c.dimension_config_id
    and a.nlp_type =  c.nlp_type
    and a.nlp_pack = c.nlp_pack
    and a.company_id = c.company_id
    and a.source_id = c.source_id
    and a.dimension = c.dimension    
    and date(a.daily_date) = date(c.daily_date)


    LEFT JOIN count_records d
    on a.case_study_id = d.case_study_id
    and a.dimension_config_id = d.dimension_config_id
    and a.nlp_type =  d.nlp_type
    and a.nlp_pack = d.nlp_pack
    and a.company_id = d.company_id
    and a.source_id = d.source_id      
    and date(a.daily_date) = date(d.daily_date)
;

"""

query_string2 = f"""

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
    split(terms,'	') AS single_terms,
    polarity,
    modified_polarity
    FROM `{project}.{dwh}.summary_table`
    WHERE dimension is not null   
    
    )

     ,single AS (SELECT  * EXCEPT(single_terms), single_terms FROM tmp, UNNEST(single_terms) AS single_terms)
        
    ,count_terms AS (SELECT 
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
    # and case_study_id =  144
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

    ,count_dimension AS 

    (SELECT 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension as dimension,
    sum(CASE WHEN modified_polarity is null THEN 0 ELSE modified_polarity END) as sum_ss,
    count(CASE WHEN (polarity != 'NONE') AND (polarity is not null) THEN  review_id ELSE null END) as sum_review_counts,
    review_date as daily_date
    
    FROM `{project}.{dwh}.summary_table`
    WHERE dimension is not null
    # and case_study_id =  144
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,    
    daily_date)

    ,count_records AS 

    (SELECT 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    
    count(distinct review_id) as records,
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
    daily_date)   
        
    select a.*
    , topic_review_counts, 
    sum_ss, sum_review_counts, records
    
    FROM count_terms a
    
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
    and date(a.daily_date) = date(b.daily_date)
    
    LEFT JOIN count_dimension c
    on a.case_study_id = c.case_study_id
    and a.dimension_config_id = c.dimension_config_id
    and a.nlp_type =  c.nlp_type
    and a.nlp_pack = c.nlp_pack
    and a.company_id = c.company_id
    and a.source_id = c.source_id
    and a.dimension = c.dimension    
    and date(a.daily_date) = date(c.daily_date)


    LEFT JOIN count_records d
    on a.case_study_id = d.case_study_id
    and a.dimension_config_id = d.dimension_config_id
    and a.nlp_type =  d.nlp_type
    and a.nlp_pack = d.nlp_pack
    and a.company_id = d.company_id
    and a.source_id = d.source_id      
    and date(a.daily_date) = date(d.daily_date)
;


"""

