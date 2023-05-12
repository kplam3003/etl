"""Create_table_case_study_run_id

Revision ID: bdcd29b62ccf
Revises: c055804cf639
Create Date: 2021-01-28 09:44:34.419577

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
revision = 'bdcd29b62ccf'
down_revision = 'c055804cf639'
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
    """create_table_case_study_run_id and add column run_id to nlp_output_casestudy and summary_table"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    

query_string1 = f"""
#Create table case_study_run_id:

CREATE OR REPLACE TABLE `{project}.{staging}.case_study_run_id` 
(     
    `created_at` timestamp,
    `case_study_id` int64,
    `run_id` string     
  )

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))

;

# Add column run_id to nlp_output_case_study:

CREATE
OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study_bk` as
SELECT
    *,
    cast(null as string) run_id
FROM
    `{project}.{dwh}.nlp_output_case_study`;

DROP TABLE `{project}.{dwh}.nlp_output_case_study`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    *
FROM
    `{project}.{dwh}.nlp_output_case_study_bk`;

DROP TABLE `{project}.{dwh}.nlp_output_case_study_bk`;


# Add column run_id to summary_table:

CREATE
OR REPLACE TABLE `{project}.{dwh}.summary_table_bk` as
SELECT
    *,
    cast(null as string) run_id
FROM
    `{project}.{dwh}.summary_table`;

DROP TABLE `{project}.{dwh}.summary_table`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.summary_table` 

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    *
FROM
    `{project}.{dwh}.summary_table_bk`;

DROP TABLE `{project}.{dwh}.summary_table_bk`;
"""


query_string2 = f"""

DROP TABLE `{project}.{staging}.case_study_run_id` ;

# DROP column run_id to nlp_output_case_study:

CREATE
OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study_bk` as
SELECT
    * except(run_id)
FROM
    `{project}.{dwh}.nlp_output_case_study`;

DROP TABLE `{project}.{dwh}.nlp_output_case_study`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    *
FROM
    `{project}.{dwh}.nlp_output_case_study_bk`;

DROP TABLE `{project}.{dwh}.nlp_output_case_study_bk`;

# DROP column run_id to summary_table:

CREATE
OR REPLACE TABLE `{project}.{dwh}.summary_table_bk` as
SELECT
    * except(run_id)
FROM
    `{project}.{dwh}.summary_table`;

DROP TABLE `{project}.{dwh}.summary_table`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.summary_table` 

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    *
FROM
    `{project}.{dwh}.summary_table_bk`;

DROP TABLE `{project}.{dwh}.summary_table_bk`;
"""