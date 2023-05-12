"""Drop unique case_study_id

Revision ID: d15a0a99ed2e
Revises: 9cbaef33c196
Create Date: 2021-02-03 10:03:18.985208

"""
from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'd15a0a99ed2e'
down_revision = '9cbaef33c196'
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
    pass

SQL_upgrade="""
ALTER TABLE mdm.etl_request 
DROP CONSTRAINT case_study_id;
"""