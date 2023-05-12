"""Add field payload for etl_request

Revision ID: 9f97c805b038
Revises: bc249f704513
Create Date: 2021-01-29 15:38:22.731989

"""
from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '9f97c805b038'
down_revision = 'bc249f704513'
branch_labels = None
depends_on = None

def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()

def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()

SQL_upgrade="""
ALTER TABLE mdm.etl_request 
ADD column payload jsonb;
"""

SQL_downgrade="""
ALTER TABLE mdm.etl_request 
DROP column payload;
"""