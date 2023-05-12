"""create view for exporting voe jobs

Revision ID: a75faf7b8745
Revises: fedec5576850
Create Date: 2022-05-30 11:37:28.964781

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import config


# revision identifiers, used by Alembic.
revision = 'a75faf7b8745'
down_revision = 'fedec5576850'
branch_labels = None
depends_on = None

# create BQ client
bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

query_create_view = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.voe_job_postings` AS 
SELECT DISTINCT
    case_study_id,
    posted_date,
    company_name,
    source_name,
    job_id,
    job_name,
    job_function,
    job_type,
    job_country,
    role_seniority
FROM `{project}.{datamart}.voe_job_summary_table`
WHERE dimension_config_name IS NOT NULL
ORDER BY posted_date DESC, job_id
"""

query_drop_view = f"""
DROP VIEW `{project}.{datamart}.voe_job_postings`
"""

def upgrade():
    """create voe_job_postings"""
    query_job = bqclient.query(query_create_view)
    query_job.result()  
    print("\n Create voe_job_postings on Datamart successfull!")


def downgrade():
    query_job = bqclient.query(query_drop_view)
    query_job.result()
    print("\n Downgrade a75faf7b8745 process is successfull!")
