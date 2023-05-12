"""add_column_review_country_to_voc

Revision ID: a21e23773801
Revises: 173b354fbcc4
Create Date: 2021-10-14 11:42:18.489539

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
revision = 'a21e23773801'
down_revision = '173b354fbcc4'
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
    print("\n Add column review_country to voc table in Staging is successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade to old version of voc table in Staging is successfull!")

query_string1 = f"""
ALTER TABLE `{project}.{staging}.voc` 
ADD COLUMN review_country string;
"""

query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voc`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
SELECT * 
EXCEPT(review_country)
FROM `{project}.{staging}.voc`;
"""

