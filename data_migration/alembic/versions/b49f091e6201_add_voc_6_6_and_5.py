"""Add_VOC_6_6_and_5

Revision ID: b49f091e6201
Revises: 8f3b4898d4b8
Create Date: 2021-01-12 11:47:53.273625

"""
from alembic import op
import sqlalchemy as sa


from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import ast
import time
import config

# revision identifiers, used by Alembic.
revision = 'b49f091e6201'
down_revision = 'c76821838ac4'
branch_labels = None
depends_on = None


#access Bigquery:
bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
### Create tables VOC_6_6 and VOC_5:
    query_string1 = f"""    
    # VIEW VOC_6_6:
    CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_6_sstimesourcecompany` AS

    WITH table1 AS (
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

    count(distinct review_id) as records,
    count(review_id) as collected_review_count,
    sum( modified_polarity ) as sum_modified_polarity

    FROM `{project}.{dwh}.summary_table`
    WHERE dimension_config_id is not null
    GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,

    daily_date)

    select 
    case_study_id,
    case_study_name,
    dimension_config_name,
    nlp_type,
    nlp_pack,
    source_name,
    company_name,
    company_id, 
    source_id,

    daily_date,
    records as records,
    collected_review_count as records_daily,
    sum_modified_polarity as ss_daily,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
    < 7 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    END AS SS_MA7,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id


    ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
    < 7 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA7,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
    < 14 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
    END AS SS_MA14,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
    < 14 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA14,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
    < 30 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    END AS SS_MA30,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
    < 30 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA30,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
    < 60 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    END AS SS_MA60,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
    < 60 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA60,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
    < 90 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    END AS SS_MA90,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
    < 90 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA90,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
    < 120 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
    END AS SS_MA120,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
    < 120 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA120,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
    < 150 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
    END AS SS_MA150,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
    < 150 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA150,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
    < 180 THEN NULL
    ELSE
    sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
    END AS SS_MA180,

    CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id

    ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
    < 180 THEN NULL
    ELSE
    sum(collected_review_count) OVER (PARTITION BY case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id
    ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
    END AS RECORDS_MA180

    from table1;

    #VIEW VOC_5:

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

"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreate tables in Datamart is successfull!")

#### Remove Order by in nlp_output table and review_raw_data table:

    query_string2 = f"""
    CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` 
    AS
    SELECT 
    user_name,
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    language,
    nlp_pack,
    nlp_type,
    review_id,
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
    polarity,
    modified_polarity as sentiment_score

    FROM 
    `{project}.{dwh}.summary_table`;

    CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` 
    AS
    SELECT 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date

    FROM 
    `{project}.{dwh}.summary_table`
    GROUP BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    language,
    review,
    trans_review,
    rating,
    review_date
    ;

    """
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nTable nlp_output and review_raw_data is updated successfully!")


def downgrade():
    query_string=f"""
    DROP VIEW `{project}.{datamart}.VOC_6_6_sstimesourcecompany` ;

    DROP VIEW `{project}.{datamart}.VOC_5_heatmapdim` ;

    CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` 
    AS
    SELECT 
    user_name,
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    language,
    nlp_pack,
    nlp_type,
    review_id,
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
    polarity,
    modified_polarity as sentiment_score

    FROM 
    `{project}.{dwh}.summary_table`

    ORDER BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    nlp_pack,
    nlp_type,
    user_name;


    CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` 
    AS
    SELECT 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date

    FROM 
    `{project}.{dwh}.summary_table`

    GROUP BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    language,
    review,
    trans_review,
    rating,
    review_date

    ORDER BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    review_date,
    review_id,
    user_name;


    """
    query_job =bqclient.query(query_string)
    query_job .result()
    print("\nDelete tables in Datamart is successfull!")
    
