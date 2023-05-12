"""create_view_voe_6_5_competitor

Revision ID: 29ee2495a56a
Revises: 648c2d3de3e5
Create Date: 2021-05-17 06:32:23.846210

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
revision = '29ee2495a56a'
down_revision = '648c2d3de3e5'
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
    print("\n Create table VOE_6_5_competitor_table successfull!")
    time.sleep(15)
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Create view VOE_6_5_competitor successfull!")
def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Drop view VOE_6_5_competitor is successfull!")
    time.sleep(15)
    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade 648c2d3de3e5 process is successfull!")

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOE_6_5_competitor_table` 
(
    `case_study_id` int64,
    `case_study_name`  string,
    `dimension_config_name`  string,
    `dimension_config_id`  int64,
    `source_name`  string,
    `company_name`  string,
    `company_id`  int64, 
	`source_id`  int64,
    `nlp_type`  string,
    `nlp_pack`  string,
	`daily_date` DATE,
    `competitor`  string,
    `records` int64,
	`processed_review_count` int64,
	`processed_review_mention` int64,
	`sum_mentioned` int64,
    `run_id` string
	)
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5));
"""
	
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_5_competitor` AS 
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

SELECT * except(run_id) FROM `{project}.{datamart}.VOE_6_5_competitor_table`
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
DROP VIEW  `{project}.{datamart}.VOE_6_5_competitor`;
"""

query_string4 = f"""
DROP TABLE `{project}.{datamart}.VOE_6_5_competitor_table` ;
"""