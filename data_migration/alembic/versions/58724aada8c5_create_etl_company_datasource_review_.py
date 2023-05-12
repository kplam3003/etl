"""create etl_company_datasource_review_stats

Revision ID: 58724aada8c5
Revises: a75faf7b8745
Create Date: 2022-06-06 14:27:02.416942

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '58724aada8c5'
down_revision = 'a75faf7b8745'
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


upgrade_sql = """
-- create table to store company_datasource_review_stats data
CREATE TABLE IF NOT EXISTS mdm.etl_company_datasource_review_stats (
    company_datasource_id   integer PRIMARY KEY,
    request_id              integer not null,
    company_id              integer not null,
    company_name            varchar(255),
    source_id               integer not null,
    source_name             varchar(255),
    source_code             varchar(255),
    source_type             varchar(100),
    created_at              timestamp,
    updated_at              timestamp,
    data_version            integer,
    progress                real,
    payload                 jsonb,
    urls                    jsonb,
    nlp_type                varchar(100),
    status                  varchar(100),
    all_request_ids         integer ARRAY,
    CONSTRAINT fk_request_id
        FOREIGN KEY(request_id) 
        REFERENCES mdm.etl_request(request_id)
);
"""

downgrade_sql = """
-- first drop the foreign key constraint
ALTER TABLE mdm.etl_company_datasource_review_stats
DROP CONSTRAINT fk_request_id;

-- then drop the table
DROP TABLE IF EXISTS mdm.etl_company_datasource_review_stats;
"""
