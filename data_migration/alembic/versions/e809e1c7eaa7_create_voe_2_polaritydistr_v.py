"""create_voe_2_polaritydistr_v

Revision ID: e809e1c7eaa7
Revises: 71209f96d642
Create Date: 2021-05-14 08:19:38.727374

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
revision = 'e809e1c7eaa7'
down_revision = '71209f96d642'
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
    print("\n Create view VOE_2_polaritydistr_v successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 71209f96d642 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_2_polaritydistr_v` AS 
SELECT 
case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
max(company_name) company_name,
company_id, 
review_date as daily_date,
polarity,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{datamart}.voe_summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
daily_date,
polarity;
"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_2_polaritydistr_v`;
"""
