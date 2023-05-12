"""create view voc_4_4

Revision ID: e64e860b1e0a
Revises: 185f4a1a529d
Create Date: 2021-03-16 04:26:57.532986

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
revision = 'e64e860b1e0a'
down_revision = '185f4a1a529d'
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
    """create view voc_4_4"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade e64e860b1e0a process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()

    print("\n Downgrade e64e860b1e0a process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_4_4_avgssyearlycompany`
AS
SELECT *
FROM `{project}.{datamart}.VOC_4_1_sstimecompany` 
;



"""
query_string2 = f"""
DROP VIEW `{project}.{datamart}.VOC_4_4_avgssyearlycompany`;


"""
