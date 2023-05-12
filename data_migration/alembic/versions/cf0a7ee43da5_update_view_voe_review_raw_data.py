"""update_view_voe_review_raw_data

Revision ID: cf0a7ee43da5
Revises: 58f294b6c13e
Create Date: 2021-07-15 04:42:44.393984

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
revision = 'cf0a7ee43da5'
down_revision = '58f294b6c13e'
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
  
    print("\n Update view voe_review_raw_data on Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 58f294b6c13e process is successfull!")
 


    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.voe_review_raw_data` AS
WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.voe_batch_status`
    WHERE
        status = 'Active'
        
) ,
parent_review as (
SELECT 
DISTINCT 
case_study_id,
review_id,
parent_review_id,
technical_type
FROM  `{project}.{staging}.voe_parent_review_mapping`
WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
        )
        
)
SELECT DISTINCT
	a.case_study_id,
	case_study_name,
	company_name,
	source_name,
	company_id, 
	source_id,
	a.review_id,
	review as orig_review,
	language,
	trans_review,
	rating,
	review_date,
	p.parent_review_id,
	p.technical_type

FROM 
`{project}.{datamart}.voe_summary_table` a
LEFT JOIN parent_review p
	ON a.case_study_id = p.case_study_id
	AND a.review_id = p.review_id
WHERE dimension_config_name is not null;

"""

 
    
query_string2 = f"""

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
WHERE dimension_config_name is not null;

"""


