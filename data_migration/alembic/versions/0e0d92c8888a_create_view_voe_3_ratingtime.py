"""create_view_voe_3_ratingtime

Revision ID: 0e0d92c8888a
Revises: e809e1c7eaa7
Create Date: 2021-05-17 06:27:09.357477

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
revision = '0e0d92c8888a'
down_revision = 'e809e1c7eaa7'
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
    print("\n Create table VOE_3_ratingtime_table successfull!")
    time.sleep(15)
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Create view VOE_3_ratingtime successfull!")

def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Drop view VOE_3_ratingtime is successfull!")
    time.sleep(15)
    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade e809e1c7eaa7 process is successfull!")

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOE_3_ratingtime_table` 
(
    `case_study_id` int64,
    `case_study_name`  string,
    `dimension_config_name`  string,
    `dimension_config_id`  int64,
    `nlp_type`  string,
    `nlp_pack`  string,
	`source_id`  int64,
    `source_name`  string,
    `company_name`  string,
    `company_id`  int64, 
	`daily_date` DATE,
    `records` int64,
	`records_daily` int64,
	`rating_daily` float64,
    `run_id` string,
	`RATING_MA7` float64,
	`RECORDS_MA7` int64,
	`RATING_MA14` float64,
	`RECORDS_MA14` int64,	
	`RATING_MA30` float64,
	`RECORDS_MA30` int64,	
	`RATING_MA60` float64,
	`RECORDS_MA60` int64,
	`RATING_MA90` float64,
	`RECORDS_MA90` int64,
	`RATING_MA120` float64,
	`RECORDS_MA120` int64,
	`RATING_MA150` float64,
	`RECORDS_MA150` int64,
	`RATING_MA180` float64,
	`RECORDS_MA180` int64
	)
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5));
"""

query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_3_ratingtime` AS 
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

SELECT * except(run_id) FROM `{project}.{datamart}.VOE_3_ratingtime_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1);
		"""
		
query_string3 = f"""
DROP VIEW  `{project}.{datamart}.VOE_3_ratingtime`;
"""
query_string4 = f"""
DROP TABLE `{project}.{datamart}.VOE_3_ratingtime_table` ;
"""