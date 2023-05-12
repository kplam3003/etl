"""create view voc_4_3

Revision ID: 185f4a1a529d
Revises: 738a3b3aa3c2
Create Date: 2021-03-16 04:22:54.626045

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
revision = '185f4a1a529d'
down_revision = '738a3b3aa3c2'
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
    """create view voc_4_3"""
    query_job =bqclient.query(query_string1)
    query_job .result()
 
    print("\n Upgrade 185f4a1a529d process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade 185f4a1a529d process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_4_3_avgsscompany`
AS
SELECT *
FROM `{project}.{datamart}.VOC_4_1_sstimecompany` 
;

"""
query_string2 = f"""
DROP VIEW `{project}.{datamart}.VOC_4_3_avgsscompany`;

"""
