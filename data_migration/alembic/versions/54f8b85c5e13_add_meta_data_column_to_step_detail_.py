"""add_meta_data_column_to_step_detail_table

Revision ID: 54f8b85c5e13
Revises: f5904b62d051
Create Date: 2021-03-24 07:32:42.735583

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '54f8b85c5e13'
down_revision = 'f5904b62d051'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Add meta_data column to mdm.step_detail table successfull!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade f5904b62d051 process is successfull!")

SQL_upgrade="""
ALTER TABLE mdm.etl_step_detail
ADD COLUMN meta_data jsonb;
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_step_detail
DROP COLUMN meta_data;

"""

