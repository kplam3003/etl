"""create table voc_6_9

Revision ID: 48f4b580018f
Revises: 82ad7aa4df49
Create Date: 2021-03-29 11:22:07.340091

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
revision = '48f4b580018f'
down_revision = '82ad7aa4df49'
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
    """create view voc_6_9"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade 48f4b580018f process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade 48f4b580018f process is successfull!")
    

query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_9_avgssdimyearlycompany`
AS
SELECT *
FROM `{project}.{datamart}.VOC_6_1_sstimedimcompany`
;

"""

query_string2 = f"""

DROP VIEW  `{project}.{datamart}.VOC_6_9_avgssdimyearlycompany`;

"""