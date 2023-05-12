"""create view voc_1_9

Revision ID: 1f445620ada3
Revises: 38a359bf5f63
Create Date: 2021-03-16 04:30:50.322814

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
revision = '1f445620ada3'
down_revision = '38a359bf5f63'
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
    """create view voc_1_9"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade 1f445620ada3 process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade 1f445620ada3 process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_9_avgcusrevyearlycompany`
AS
SELECT *
FROM `{project}.{datamart}.VOC_1_1_cusrevtimecompany_v` 
;

"""
query_string2 = f"""

DROP VIEW `{project}.{datamart}.VOC_1_9_avgcusrevyearlycompany`;

"""
