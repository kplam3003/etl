"""add column all_request_ids to mdm.etl_company_datasource

Revision ID: 9258198b685c
Revises: fa5f5698f743
Create Date: 2021-07-27 09:39:53.902106

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '9258198b685c'
down_revision = 'fa5f5698f743'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Added all_request_ids column to mdm.etl_company_datasource successfully!")


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
ALTER TABLE mdm.etl_company_datasource
    ADD COLUMN all_request_ids      integer ARRAY;
"""

downgrade_sql = """
ALTER TABLE mdm.etl_company_datasource
    DROP COLUMN all_request_ids
"""
