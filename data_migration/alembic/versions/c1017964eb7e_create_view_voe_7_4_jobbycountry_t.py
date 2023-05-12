"""create_view_VOE_7_4_jobbycountry_t

Revision ID: c1017964eb7e
Revises: 5163691cdd7c
Create Date: 2021-04-15 04:02:49.638312

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
revision = 'c1017964eb7e'
down_revision = '5163691cdd7c'
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
    """create view VOE_7_4_jobbycountry_t"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create view VOE_7_4_jobbycountry_t in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 5163691cdd7c process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_7_4_jobbycountry_t` AS
SELECT 
    case_study_id, 
    case_study_name,
    nlp_pack,
    nlp_type, 
    dimension_config_id, 
    dimension_config_name,
    source_name,
    source_id,
    company_name,
    company_id,
    DATE(posted_date) as posted_date,
    CASE 
    WHEN job_country is NULL or job_country = '' THEN 'undefined'
    ELSE job_country 
    END AS job_country,
    COUNT(DISTINCT job_id) as job_quantity,
    MAX(fte) as fte
  
FROM `{project}.{datamart}.voe_job_summary_table` a
GROUP BY
    case_study_id, 
    case_study_name,
    nlp_pack,
    nlp_type, 
    dimension_config_id, 
    dimension_config_name,
    source_name,
    source_id,
    company_name,
    company_id,
    DATE(posted_date),
    job_country;
        """

query_string2 = f"""
DROP VIEW `{project}.{datamart}.VOE_7_4_jobbycountry_t` ;

        """

