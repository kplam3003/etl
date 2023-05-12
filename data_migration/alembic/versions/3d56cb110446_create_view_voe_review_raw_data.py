"""create_view_voe_review_raw_data

Revision ID: 3d56cb110446
Revises: 304f32d5e822
Create Date: 2021-04-15 09:09:25.761322

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
revision = '3d56cb110446'
down_revision = '304f32d5e822'
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
    """create voe_review_raw_data"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create voe_review_raw_data on Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 304f32d5e822 process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.voe_review_raw_data` AS
SELECT DISTINCT
case_study_id,
case_study_name,
company_name,
source_name,
company_id, 
source_id,
review_id,
review as orig_review,
language,
trans_review,
rating,
review_date

FROM 
`{project}.{datamart}.voe_summary_table`
WHERE dimension_config_name is not null

"""

query_string2 = f"""
DROP VIEW `{project}.{datamart}.voe_review_raw_data` ;

"""
