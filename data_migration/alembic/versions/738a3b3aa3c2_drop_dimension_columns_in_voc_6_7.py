"""drop dimension columns in VOC_6_7

Revision ID: 738a3b3aa3c2
Revises: 07ebad4a36bc
Create Date: 2021-03-12 04:00:54.814851

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
revision = '738a3b3aa3c2'
down_revision = '07ebad4a36bc'
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
    """drop dimension columns in VOC_6_7"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    time.sleep(5)

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Upgrade 738a3b3aa3c2 process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    time.sleep(5)

    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade 738a3b3aa3c2 process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_6_7_commonterms_table_bk` 
AS
WITH tmp AS (
    SELECT 
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
        modified_polarity,
        run_id
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null    
        and dimension_config_name is not null
        and is_used = true

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
        polarity,
        single_terms,
        review_date as daily_date,
        count(distinct review_id) as records,
        count(distinct review_id) as collected_review_count,
        run_id
    FROM single
    GROUP BY 
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id, 
        source_id,
        polarity,
        single_terms,
        daily_date,
        run_id
;

DROP TABLE `{project}.{datamart}.VOC_6_7_commonterms_table`;

"""
query_string2 = f"""

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_6_7_commonterms_table`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5))
AS 
SELECT * FROM `{project}.{datamart}.VOC_6_7_commonterms_table_bk`;

DROP TABLE `{project}.{datamart}.VOC_6_7_commonterms_table_bk` ;

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_7_commonterms`
AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.case_study_run_id`
)

SELECT * except(run_id) FROM `{project}.{datamart}.VOC_6_7_commonterms_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    );

"""

query_string3 = f"""

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_6_7_commonterms_table_bk` 
AS
WITH tmp AS (
    SELECT 
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
        modified_polarity,
        run_id
    FROM `{project}.{datamart}.summary_table`
    WHERE dimension is not null    
        and dimension_config_name is not null
        and is_used = true

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
        modified_dimension as dimension,
        polarity,
        single_terms,
        review_date as daily_date,
        count(distinct review_id) as records,
        count(distinct review_id) as collected_review_count,
        run_id
    FROM single
    GROUP BY 
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id, 
        source_id,
        modified_dimension,
        polarity,
        single_terms,
        daily_date,
        run_id
;

DROP TABLE `{project}.{datamart}.VOC_6_7_commonterms_table`;


"""
query_string4 = f"""

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_6_7_commonterms_table`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5))
AS 
SELECT * FROM `{project}.{datamart}.VOC_6_7_commonterms_table_bk`;

DROP TABLE `{project}.{datamart}.VOC_6_7_commonterms_table_bk` ;

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_7_commonterms`
AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.case_study_run_id`
)

SELECT * except(run_id) FROM `{project}.{datamart}.VOC_6_7_commonterms_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    );

"""