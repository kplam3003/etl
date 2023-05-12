"""Create view VOE_12_2_avgratingperiodcompany

Revision ID: 9176a7458b4d
Revises: 687a1d946ed9
Create Date: 2022-07-04 14:37:56.153401

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9176a7458b4d'
down_revision = '687a1d946ed9'
branch_labels = None
depends_on = None

from google.cloud import bigquery
import config

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
datamart = config.DATAMART

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create view VOE_12_2_avgratingperiodcompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print(f"\n Downgrade {revision} process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_12_2_avgratingperiodcompany` AS 
SELECT *
FROM `{project}.{datamart}.VOE_12_ratingtimecompany` ;
"""
		
query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_12_2_avgratingperiodcompany`;
"""