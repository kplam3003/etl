"""create_view_voc_13_1_avgratingcompany

Revision ID: ddd20afaf6b7
Revises: 3e1c2b89ab32
Create Date: 2021-05-20 04:34:27.833192

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
revision = 'ddd20afaf6b7'
down_revision = '3e1c2b89ab32'
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
    print("\n Create view VOC_13_1_avgratingcompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 3e1c2b89ab32 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_13_1_avgratingcompany` AS 
SELECT *
FROM `{project}.{datamart}.VOC_13_ratingtimecompany` ;
"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOC_13_1_avgratingcompany`;
"""
