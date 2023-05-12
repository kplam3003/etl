"""add nlp_type column to mdm.etl_company_datasource

Revision ID: 4ebceedf11cf
Revises: 88620e60b77b
Create Date: 2021-07-26 21:03:35.102942

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '4ebceedf11cf'
down_revision = '88620e60b77b'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Added nlp_type column to mdm.etl_company_datasource successfully!")


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
-- add nlp type column
ALTER TABLE mdm.etl_company_datasource
    ADD COLUMN nlp_type         varchar(100);
"""

downgrade_sql = """
-- drop nlp_type column
ALTER TABLE mdm.etl_company_datasource
    DROP COLUMN nlp_type;
"""
