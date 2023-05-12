"""Create view VOE_12_1_avgratingcompany

Revision ID: 687a1d946ed9
Revises: 29a2979f980f
Create Date: 2022-07-04 14:36:55.869194

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '687a1d946ed9'
down_revision = '29a2979f980f'
branch_labels = None
depends_on = None

from google.cloud import bigquery
import config

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
datamart = config.DATAMART

query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_12_1_avgratingcompany` AS 
SELECT *
FROM `{project}.{datamart}.VOE_12_ratingtimecompany`;
"""

query_string2 = f"""
DROP VIEW  `{project}.{datamart}.VOE_12_1_avgratingcompany`;
"""

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create view VOE_12_1_avgratingcompany successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print(f"\n Downgrade {revision} process is successfull!")