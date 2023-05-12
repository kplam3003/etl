"""add_metadata_to_batch

Revision ID: 065bd7be63f0
Revises: 4cabe518212b
Create Date: 2021-03-29 11:54:52.832189

"""
from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '065bd7be63f0'
down_revision = '4cabe518212b'
branch_labels = None
depends_on = None

def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Add meta_data column to mdm.etl_company_batch table successfull!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade process is successfull!")


SQL_upgrade="""
ALTER TABLE mdm.etl_company_batch
ADD COLUMN meta_data jsonb;
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_company_batch
DROP COLUMN meta_data;
"""
