"""add data_version_jobs to mdm tables

Revision ID: e51022f6c51d
Revises: 3c80b22acadc
Create Date: 2021-11-08 11:31:54.581074

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = 'e51022f6c51d'
down_revision = '3c80b22acadc'
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
        "\n Add column data_version_jobs columns to table "
        "etl_company_datasource, etl_company_batch, "
        "etl_case_study_data successful!"
    )


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade e51022f6c51d process is successful!")

SQL_upgrade="""
ALTER TABLE mdm.etl_company_datasource
ADD COLUMN data_version_job integer;
UPDATE mdm.etl_company_datasource
SET data_version_job = data_version
WHERE 1 = 1;

ALTER TABLE mdm.etl_company_batch
ADD COLUMN data_version_job integer;
UPDATE mdm.etl_company_batch
SET data_version_job = data_version
WHERE 1 = 1;

ALTER TABLE mdm.etl_case_study_data
ADD COLUMN data_version_job integer;
UPDATE mdm.etl_case_study_data
SET data_version_job = data_version
WHERE 1 = 1;
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_company_datasource
DROP COLUMN data_version_job;

ALTER TABLE mdm.etl_company_batch
DROP COLUMN data_version_job;

ALTER TABLE mdm.etl_case_study_data
DROP COLUMN data_version_job;
"""
