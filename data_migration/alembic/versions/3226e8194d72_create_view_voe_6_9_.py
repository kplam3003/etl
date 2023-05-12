"""create_view_voe_6_9_avgssdimyearlycompany

Revision ID: 3226e8194d72
Revises: 1fecc0a47175
Create Date: 2021-05-17 06:31:00.542962

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
revision = '3226e8194d72'
down_revision = '1fecc0a47175'
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
    print("\n Create view VOE_6_9_avgssdimyearlycompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 1fecc0a47175 process is successfull!")

query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_9_avgssdimyearlycompany` AS 
SELECT *
FROM `{project}.{datamart}.VOE_6_1_sstimedimcompany`;
"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_6_9_avgssdimyearlycompany`;
"""