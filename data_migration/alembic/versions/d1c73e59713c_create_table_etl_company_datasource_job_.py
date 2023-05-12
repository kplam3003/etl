"""create table etl_company_datasource_job to store job-related metadata

Revision ID: d1c73e59713c
Revises: d401c039c353
Create Date: 2021-11-09 21:55:01.244901

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = 'd1c73e59713c'
down_revision = 'd401c039c353'
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
-- create table to store company_datasource_job data
CREATE TABLE IF NOT EXISTS mdm.etl_company_datasource_job (
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
"""

downgrade_sql = """
-- first drop the foreign key constraint
ALTER TABLE mdm.etl_company_datasource_job
DROP CONSTRAINT fk_request_id;

-- then drop the table
DROP TABLE IF EXISTS mdm.etl_company_datasource_job;
"""
