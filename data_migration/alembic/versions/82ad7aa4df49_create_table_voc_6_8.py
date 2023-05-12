"""create table voc_6_8

Revision ID: 82ad7aa4df49
Revises: 4cabe518212b
Create Date: 2021-03-29 11:21:52.385220

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
revision = '82ad7aa4df49'
down_revision = '065bd7be63f0'
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
    """create view voc_6_8"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade 82ad7aa4df49 process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade 82ad7aa4df49 process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_8_avgssdimcompany`
AS
SELECT *
FROM `{project}.{datamart}.VOC_6_1_sstimedimcompany`
;

"""
query_string2 = f"""

DROP VIEW  `{project}.{datamart}.VOC_6_8_avgssdimcompany`;

"""