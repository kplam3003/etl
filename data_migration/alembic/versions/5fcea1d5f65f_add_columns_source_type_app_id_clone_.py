"""add columns source_type, app_id, clone_from_cs to ETLCompanyBatch and ETLRequest

Revision ID: 5fcea1d5f65f
Revises: 2ddb60636e27
Create Date: 2021-03-02 06:52:39.535461

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '5fcea1d5f65f'
down_revision = '2ddb60636e27'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(query_SQL1)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Upgrade 5fcea1d5f65f process is successfull!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(query_SQL2)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade 5fcea1d5f65f process is successfull!")


query_SQL1 = """
---Add extra columns to ETLCompanyBatch table
ALTER TABLE mdm.etl_company_batch 
ADD COLUMN source_type VARCHAR(255),
ADD COLUMN app_id VARCHAR(255);

---Add extra column to ETLRequest table
ALTER TABLE mdm.etl_request
ADD COLUMN clone_from_cs_id VARCHAR(255);

"""
query_SQL2 = """
---Add extra columns to ETLCompanyBatch table
ALTER TABLE mdm.etl_company_batch 
DROP COLUMN source_type, 
DROP COLUMN app_id ;

---Add extra column to ETLRequest table
ALTER TABLE mdm.etl_request
DROP COLUMN clone_from_cs_id;

"""