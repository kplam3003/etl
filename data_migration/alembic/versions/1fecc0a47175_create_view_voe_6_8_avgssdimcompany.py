"""create_view_voe_6_8_avgssdimcompany

Revision ID: 1fecc0a47175
Revises: 531682a4d07c
Create Date: 2021-05-17 06:30:31.506297

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
revision = '1fecc0a47175'
down_revision = '531682a4d07c'
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
    print("\n Create view VOE_6_8_avgssdimcompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 531682a4d07c process is successfull!")

	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_8_avgssdimcompany` AS 
SELECT *
FROM `{project}.{datamart}.VOE_6_1_sstimedimcompany`;
"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_6_8_avgssdimcompany`;
"""