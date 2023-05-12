"""create_view_voe_6_2_dimentioned

Revision ID: f5b8783e86c1
Revises: 95f9c80f1f4c
Create Date: 2021-05-13 07:24:40.960051

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
revision = 'f5b8783e86c1'
down_revision = '95f9c80f1f4c'
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
    print("\n Create view VOE_6_2_dimentioned successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 95f9c80f1f4c process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_2_dimentioned` AS 
SELECT
	case_study_id,
	max(case_study_name) case_study_name,
	max(dimension_config_name) dimension_config_name,
	dimension_config_id,
	nlp_type,
	nlp_pack,
	max(source_name) source_name,
	max(company_name) company_name,
	company_id, 
	source_id,
	dimension_type,
	modified_dimension as dimension,
	review_date as daily_date,
	count(distinct review_id) as records,
	count(distinct review_id) as collected_review_count

FROM `{project}.{datamart}.voe_summary_table`
WHERE
dimension_config_name is not null
AND dimension is not null
AND is_used = true

GROUP BY 
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id,
dimension_type,
modified_dimension,
daily_date;		
		"""

query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_6_2_dimentioned`;
    """

