"""update_view_voe_11_termcount

Revision ID: e6756bc1b41c
Revises: c3f0393cf120
Create Date: 2021-05-10 02:36:17.886257

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
revision = 'e6756bc1b41c'
down_revision = 'c3f0393cf120'
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
    print("\n Update view VOE_11_termcount successfull!")
    time.sleep(15)


def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade c3f0393cf120 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_11_termcount` AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.voe_case_study_run_id`
)

SELECT * except(run_id) FROM `{project}.{datamart}.VOE_11_termcount_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )
;
"""

query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_11_termcount` AS
SELECT * except(run_id) FROM `{project}.{datamart}.VOE_11_termcount_table`;

"""
	
