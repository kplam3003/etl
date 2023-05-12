"""create_table_casestudy_batchid_BQ

Revision ID: ad945597de9c
Revises: e924b47c2a45
Create Date: 2021-01-15 06:51:33.143634

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
revision = 'ad945597de9c'
down_revision = 'e924b47c2a45'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)



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
    print("\nCreating table in Staging is successfull!")

def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDrop table in Staging is successfull!")

query_string1 = f"""
# Create table casestudy_batchid:
CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid`
(
     
    `created_at` timestamp,
    `case_study_id` int64,
    `request_id` int64,
    `batch_id`  int64
)
;"""

query_string2 = f"""
DROP TABLE `{project}.{staging}.casestudy_batchid`
;"""
