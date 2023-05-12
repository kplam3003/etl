"""Add request_type to mdm.etl_request

Revision ID: 49818487ece2
Revises: a9de20c13085
Create Date: 2021-07-23 17:41:09.885468

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '49818487ece2'
down_revision = 'a9de20c13085'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Add column request_type to table etl_request and back-fill data successfully!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(downgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n 49818487ece2 downgraded successfully!")



upgrade_sql = """
ALTER TABLE mdm.etl_request
ADD COLUMN request_type character varying(255);
"""

downgrade_sql = """
ALTER TABLE mdm.etl_request
DROP COLUMN request_type;
"""