"""create_view_voe_11_1_keywordcount

Revision ID: 872980087278
Revises: 01ff20548c4c
Create Date: 2021-05-31 04:48:49.970378

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
revision = '872980087278'
down_revision = '01ff20548c4c'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS
 

def upgrade():
 
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create table VOE_11_1_keywordcount_table successfull!")
    time.sleep(15)

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Create view VOE_11_1_keywordcount successfull!")

def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Drop view VOE_11_1_keywordcount successfull!")
    time.sleep(15)

    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade 01ff20548c4c process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOE_11_1_keywordcount_table` 
(
    `case_study_id` int64,
    `case_study_name`  string,
    `dimension_config_name`  string,
    `dimension_config_id`  int64,
    `nlp_type`  string,
    `nlp_pack`  string,
    `source_name`  string,
    `company_name`  string,
    `company_id`  int64, 
    `source_id`  int64,
    `polarity`  string,
    `single_terms`  string,
    `daily_date` DATE,
    `records` int64,
    `collected_review_count` int64,
    `run_id` string)
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5));
"""

query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_11_1_keywordcount` AS
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
        `{project}.{staging}.voe_case_study_run_id`
)

SELECT * except(run_id) FROM `{project}.{datamart}.VOE_11_1_keywordcount_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )
;

"""
	

query_string3 = f"""
DROP VIEW  `{project}.{datamart}.VOE_11_1_keywordcount`;
"""
query_string4 = f"""
DROP TABLE `{project}.{datamart}.VOE_11_1_keywordcount_table` ;
"""
