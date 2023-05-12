"""add status to mdm.etl_company_datasource

Revision ID: fa5f5698f743
Revises: 4ebceedf11cf
Create Date: 2021-07-26 21:39:24.001939

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'fa5f5698f743'
down_revision = '4ebceedf11cf'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Added status column to mdm.etl_company_datasource successfully!")


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
-- add status column
ALTER TABLE mdm.etl_company_datasource
    ADD COLUMN status         varchar(100);
"""

downgrade_sql = """
-- drop status column
ALTER TABLE mdm.etl_company_datasource
    DROP COLUMN status;
"""