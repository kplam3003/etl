"""change datatype of data_type and url fields in mdm.etl_company_datasource

Revision ID: 88620e60b77b
Revises: be1998e8861c
Create Date: 2021-07-26 17:30:23.593704

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '88620e60b77b'
down_revision = 'be1998e8861c'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Dropped `url` and added `urls` to store JSONB successfully!")


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
-- drop old url column and add new urls column to store a json
ALTER TABLE mdm.etl_company_datasource
    DROP COLUMN url,
    ADD COLUMN urls         jsonb;
"""

downgrade_sql = """
-- drop urls and readd url
ALTER TABLE mdm.etl_company_datasource
    DROP COLUMN urls,
    ADD COLUMN url      varchar;

"""