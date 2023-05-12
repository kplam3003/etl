"""add data_type column to etl_request

Revision ID: d401c039c353
Revises: e51022f6c51d
Create Date: 2021-11-09 11:47:02.218189

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'd401c039c353'
down_revision = 'e51022f6c51d'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()
    print(
        "\n Add column data_type columns to table etl_request!"
    )


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade to e51022f6c51d process successfully!")

SQL_upgrade="""
-- add new column
ALTER TABLE mdm.etl_request
ADD COLUMN data_type character varying(255);

-- update column to fill legacy values
-- update for case_study type
UPDATE mdm.etl_request er
SET data_type = er.payload -> 'dimension_config' -> 'nlp_type'
WHERE request_type = 'case_study'
    OR request_type IS NULL;
-- update for company_datasource type
UPDATE mdm.etl_request er
SET data_type = 'all'
WHERE request_type = 'company_datasource';
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_request
DROP COLUMN data_type;
"""
