"""create table dimension_keyword_list

Revision ID: 068d27ef9640
Revises: 48f4b580018f
Create Date: 2021-03-31 09:09:45.289524

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
revision = '068d27ef9640'
down_revision = '48f4b580018f'
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
    """create view dimension_keyword_list"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade 068d27ef9640 process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade 068d27ef9640 process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE table `{project}.{dwh}.dimension_keyword_list`
            (
            `created_at` timestamp,
            `case_study_id` int64,
            `in_list` string,
            `ex_list` string,
            `run_id` string
         
            )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 

;

"""

    
query_string2 = f"""
DROP table `{project}.{dwh}.dimension_keyword_list`;

"""
