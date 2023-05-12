"""add change tracking columns to mdm.etl_case_study_data

Revision ID: 7e7b70fe951d
Revises: 9258198b685c
Create Date: 2021-07-27 13:45:17.924407

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '7e7b70fe951d'
down_revision = '9258198b685c'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Added created_at, updated_at, status columns to mdm.etl_case_study_data successfully!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(downgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print(f"\n {revision} downgraded successfully!")


# sqls
upgrade_sql = """
ALTER TABLE mdm.etl_case_study_data
    ADD COLUMN created_at       timestamp,
    ADD COLUMN updated_at       timestamp,
    ADD COLUMN status           varchar(100);
"""

downgrade_sql = """
ALTER TABLE mdm.etl_case_study_data
    DROP COLUMN created_at,
    DROP COLUMN updated_at,
    DROP COLUMN status;
"""