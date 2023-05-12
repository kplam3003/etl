"""Consolidate etl_company_datasource tables

Revision ID: 516779ad42cd
Revises: 8d94e92bfe45
Create Date: 2022-08-03 15:34:31.696415

"""
from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection
import logging


# revision identifiers, used by Alembic.
revision = '516779ad42cd'
down_revision = '8d94e92bfe45'
branch_labels = None
depends_on = None

upgrade_sql = """
-- Upgrade
-- 1. Backup table "mdm.etl_company_datasource"
CREATE TABLE mdm.etl_company_datasource_bk1
AS TABLE mdm.etl_company_datasource;

-- 2. Drop foreign keys that referencing to column "company_datasource_id" on "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_case_study_data
DROP CONSTRAINT IF EXISTS fk_company_datasource_id;

-- 3. Drop primary key on "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_company_datasource
DROP CONSTRAINT etl_company_datasource_pkey;

-- 4. Add column id, data_type to table "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_company_datasource
ADD COLUMN id SERIAL;
ALTER TABLE mdm.etl_company_datasource
ADD COLUMN data_type VARCHAR(255);

-- 5. Set data_type='review' for all rows in table "mdm.etl_company_datasource"
UPDATE mdm.etl_company_datasource
SET data_type = 'review';

-- 6. Copy all rows from "mdm.etl_company_datasource_job" to "mdm.etl_company_datasource" with data_type='job' 
INSERT INTO mdm.etl_company_datasource(
    company_datasource_id, request_id, company_id, company_name, source_id, source_name, source_code, source_type,
    created_at, updated_at, data_version, progress, payload, urls, nlp_type, status, all_request_ids,
    data_type
)
SELECT company_datasource_id, request_id, company_id, company_name, source_id, source_name, source_code, source_type,
    created_at, updated_at, data_version, progress, payload, urls, nlp_type, status, all_request_ids,
    'job'
FROM mdm.etl_company_datasource_job;

-- 7. Copy all rows from "mdm.etl_company_datasource_review_stats" to "mdm.etl_company_datasource" with data_type='review_stats'
INSERT INTO mdm.etl_company_datasource(
    company_datasource_id, request_id, company_id, company_name, source_id, source_name, source_code, source_type,
    created_at, updated_at, data_version, progress, payload, urls, nlp_type, status, all_request_ids,
    data_type
)
SELECT company_datasource_id, request_id, company_id, company_name, source_id, source_name, source_code, source_type,
    created_at, updated_at, data_version, progress, payload, urls, nlp_type, status, all_request_ids,
    'review_stats'
FROM mdm.etl_company_datasource_review_stats;

-- 8. Create new primary key on "id" for "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_company_datasource
ADD PRIMARY KEY(id);

-- 9.Make unique constraint on "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_company_datasource
ADD CONSTRAINT unique_company_datasource_id_data_type
UNIQUE(company_datasource_id, data_type);

-- 10. Re-create foreign keys to "mdm.etl_company_datasource"
ALTER TABLE mdm.etl_case_study_data
ADD CONSTRAINT fk_company_datasource_id_data_type
FOREIGN KEY (company_datasource_id, data_type) REFERENCES mdm.etl_company_datasource(company_datasource_id, data_type);

-- 11. Backup "mdm.etl_company_datasource_job" and "mdm.etl_company_datasource_review_stats"
ALTER TABLE mdm.etl_company_datasource_job RENAME TO etl_company_datasource_job_bk1;
ALTER TABLE mdm.etl_company_datasource_review_stats RENAME TO etl_company_datasource_review_stats_bk1;
"""

downgrade_sql = """
-- Downgrade
ALTER TABLE mdm.etl_case_study_data
DROP CONSTRAINT fk_company_datasource_id_data_type;

DROP TABLE mdm.etl_company_datasource;

ALTER TABLE mdm.etl_company_datasource_bk1 RENAME TO mdm.etl_company_datasource;

ALTER TABLE mdm.etl_case_study_data
ADD CONSTRAINT fk_company_datasource_id
    FOREIGN KEY (company_datasource_id)
    REFERENCES mdm.etl_company_datasource (company_datasource_id);
    
ALTER TABLE mdm.etl_company_datasource_job_bk1 RENAME TO etl_company_datasource_job;

ALTER TABLE mdm.etl_company_datasource_review_stats_bk1 RENAME TO etl_company_datasource_review_stats;
"""

def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    logging.info("Consolidating etl_company_datasource tables successfully!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(downgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    logging.info(f"Downgrade from {revision} to {down_revision} successfully!")
