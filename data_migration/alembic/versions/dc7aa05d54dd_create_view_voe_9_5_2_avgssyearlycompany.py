"""create_view_voe_9_5_2_avgssyearlycompany

Revision ID: dc7aa05d54dd
Revises: c087a310a815
Create Date: 2021-05-04 04:42:48.844488

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
revision = 'dc7aa05d54dd'
down_revision = 'c087a310a815'
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
    print("\n Create view VOE_9_5_2_avgssyearlycompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade c087a310a815 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_9_5_2_avgssyearlycompany` AS 
SELECT *
FROM `{project}.{datamart}.VOE_9_5_ssbytimecompany` ;
		"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_9_5_2_avgssyearlycompany`;
    """