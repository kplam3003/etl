"""change_sources_of_review_raw_data

Revision ID: 5f5291583b51
Revises: 9579c646dbee
Create Date: 2021-02-01 07:02:56.482221

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
revision = '5f5291583b51'
down_revision = '9579c646dbee'
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
    """change_sources_of_review_raw_data"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` 
AS
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
`{project}.{datamart}.summary_table`
WHERE dimension_config_name is not null;

"""

query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` 
AS
SELECT 
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
`{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY 
case_study_id,
case_study_name,
company_name,
source_name,
company_id, 
source_id,
review_id,
language,
review,
trans_review,
rating,
review_date;

"""

