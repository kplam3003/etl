"""Add company_datasource_id and data_version to mdm.etl_company_batch

Revision ID: be1998e8861c
Revises: 46e4e226e51b
Create Date: 2021-07-24 00:51:11.066640

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = 'be1998e8861c'
down_revision = '46e4e226e51b'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Columns company_datasource_id and data_version added to mdm.etl_company_batch successfully!")


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
ALTER TABLE mdm.etl_company_batch
ADD COLUMN company_datasource_id            integer,
ADD COLUMN data_version                     integer;
"""

downgrade_sql = """
ALTER TABLE mdm.etl_company_batch
DROP COLUMN company_datasource_id,
DROP COLUMN data_version;
"""
