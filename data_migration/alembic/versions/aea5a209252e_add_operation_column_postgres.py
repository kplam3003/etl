"""add_operation_column_postgres

Revision ID: aea5a209252e
Revises: b49f091e6201
Create Date: 2021-01-14 11:27:25.042139

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'aea5a209252e'
down_revision = 'b49f091e6201'
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
ALTER TABLE mdm.etl_company_batch 
ADD column operation varchar;

ALTER TABLE mdm.etl_step 
ADD column operation varchar;

ALTER TABLE mdm.etl_request 
ADD column operation varchar;

ALTER TABLE mdm.etl_step_detail 
ADD column operation varchar;
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_company_batch 
DROP column operation;

ALTER TABLE mdm.etl_step 
DROP column operation;

ALTER TABLE mdm.etl_request 
DROP column operation;

ALTER TABLE mdm.etl_step_detail 
DROP column operation;
"""






