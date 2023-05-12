"""create_view_voe_9_3_ssaverage

Revision ID: 6b9d4873a800
Revises: 9c45fb5065b2
Create Date: 2021-04-27 08:49:18.532062

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
revision = '6b9d4873a800'
down_revision = '9c45fb5065b2'
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
 
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create view VOE_9_3_ssaverage successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 9c45fb5065b2 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_9_3_ssaverage` AS 
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
    review_date as daily_date,
    sum(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE modified_polarity END) as sum_ss,
    count(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE review_id END) as sum_review_count

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
    review_date;
				
		"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_9_3_ssaverage`;
			"""