"""Create mdm.etl_company_datasource

Revision ID: 46e4e226e51b
Revises: 82f03f12c5ee
Create Date: 2021-07-24 00:38:03.785688

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '46e4e226e51b'
down_revision = '82f03f12c5ee'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Table mdm.etl_company_datasource created successfully!")


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
-- create table to store company_datasource data
CREATE TABLE IF NOT EXISTS mdm.etl_company_datasource (
    company_datasource_id   integer PRIMARY KEY,
    request_id              integer not null,
    company_id              integer not null,
    company_name            varchar(255),
    source_id               integer not null,
    source_name             varchar(255),
    source_code             varchar(255),
    source_type             varchar(100),
    url                     varchar,
    created_at              timestamp,
    updated_at              timestamp,
    data_version            integer,
    progress                real,
    payload					jsonb,
    CONSTRAINT fk_request_id
        FOREIGN KEY(request_id) 
        REFERENCES mdm.etl_request(request_id)
);

-- add foreign key constraint to etl_case_study_data
ALTER TABLE mdm.etl_case_study_data
ADD CONSTRAINT fk_company_datasource_id 
    FOREIGN KEY (company_datasource_id) 
    REFERENCES mdm.etl_company_datasource(company_datasource_id);
"""

downgrade_sql = """
-- first drop the foreign key constraint in mdm.etl_case_study_data table
ALTER TABLE mdm.etl_case_study_data
DROP CONSTRAINT fk_company_datasource_id;

-- then drop the table
DROP TABLE IF EXISTS mdm.etl_company_datasource;
"""
