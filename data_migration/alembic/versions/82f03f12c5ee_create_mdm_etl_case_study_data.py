"""Create mdm.etl_case_study_data

Revision ID: 82f03f12c5ee
Revises: 323ae6d11529
Create Date: 2021-07-23 23:14:08.164515

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '82f03f12c5ee'
down_revision = '323ae6d11529'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Table mdm.etl_case_study_data created successfully!")


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
-- create table to store company metadata of a case study
CREATE TABLE IF NOT EXISTS mdm.etl_case_study_data (
    case_study_id			integer not null,
    request_id				integer not null,
    company_datasource_id   integer not null,
    data_version            integer,
    nlp_type                varchar(100),
    nlp_pack                varchar(200),
    re_scrape               bool,
    re_nlp                  bool,
    progress                real,
    payload					jsonb,
    CONSTRAINT fk_case_study_id
        FOREIGN KEY(case_study_id) 
        REFERENCES mdm.etl_case_study(case_study_id)
);
"""

downgrade_sql = """
DROP TABLE IF EXISTS mdm.etl_case_study_data;
"""