"""create_view_voe_10_1_dimss

Revision ID: 30f7d7cd6330
Revises: 6b9d4873a800
Create Date: 2021-04-29 03:51:24.878032

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
revision = '30f7d7cd6330'
down_revision = '6b9d4873a800'
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
    print("\n Create view VOE_10_1_dimss successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 6b9d4873a800 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_10_1_dimss` AS 
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

    SUM(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE modified_polarity END) as sum_ss,
    COUNT(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE review_id END) as sum_review_count

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
    daily_date	;
			
			
		"""
		
query_string2 = f"""
		DROP VIEW  `{project}.{datamart}.VOE_10_1_dimss`;
			"""