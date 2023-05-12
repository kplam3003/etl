"""populate etl_company_datasource_job table with old data

Revision ID: 2f7f917fa9b4
Revises: acc042071b3d
Create Date: 2021-11-10 15:23:24.579055

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '2f7f917fa9b4'
down_revision = 'acc042071b3d'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Migrate data to etl_company_datasource_job successfully!")


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
-- create new column
ALTER TABLE mdm.etl_company_datasource_job
ADD COLUMN nlp_type CHARACTER VARYING(255),
ADD COLUMN urls JSONB,
ADD COLUMN status CHARACTER VARYING(100),
ADD COLUMN all_request_ids integer ARRAY,
DROP COLUMN url;

-- insert into mdm.etl_company_datasource_job
INSERT INTO mdm.etl_company_datasource_job
(
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    progress,
    payload,
    urls,
    nlp_type,
    status,
    all_request_ids
)
SELECT 
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version_job AS data_version,
    progress,
    payload,
    jsonb_agg(new_url) AS urls,
    nlp_type,
    status,
    all_request_ids
FROM (
    SELECT 
        company_datasource_id,
        request_id,
        company_id,
        company_name,
        source_id,
        source_name,
        source_code,
        source_type,
        created_at,
        updated_at,
        data_version,
        progress,
        payload,
        nlp_type,
        status,
        all_request_ids ,
        data_version_job ,
        jsonb_array_elements(urls) AS new_url
    FROM mdm.etl_company_datasource ecd 
    WHERE nlp_type = 'VoE'
) t
WHERE new_url ->> 'type' != 'review'
GROUP BY 
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    progress,
    payload,
    nlp_type,
    status,
    all_request_ids,
    data_version_job;

-- create a backup for mdm.etl_compay_datasource
CREATE TABLE IF NOT EXISTS mdm.etl_company_datasource_bk AS
    SELECT * FROM mdm.etl_company_datasource;
    
-- drop foreign key constraint in mdm.etl_case_study_data
ALTER TABLE mdm.etl_case_study_data 
DROP CONSTRAINT IF EXISTS fk_company_datasource_id;

-- modify existing data from mdm.etl_company_datasource
-- truncate first
TRUNCATE TABLE mdm.etl_company_datasource;
-- insert modified data
INSERT INTO mdm.etl_company_datasource
(
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    data_version_job,
    progress,
    payload,
    urls,
    nlp_type,
    status,
    all_request_ids
)
SELECT 
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    data_version_job,
    progress,
    payload,
    jsonb_agg(new_url) AS urls,
    nlp_type,
    status,
    all_request_ids
FROM (
    SELECT 
        company_datasource_id,
        request_id,
        company_id,
        company_name,
        source_id,
        source_name,
        source_code,
        source_type,
        created_at,
        updated_at,
        data_version,
        progress,
        payload,
        nlp_type,
        status,
        all_request_ids ,
        data_version_job ,
        jsonb_array_elements(urls) AS new_url
    FROM mdm.etl_company_datasource_bk ecd 
) t
WHERE new_url ->> 'type' = 'review'
GROUP BY 
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    progress,
    payload,
    nlp_type,
    status,
    all_request_ids,
    data_version_job;
    
-- re-add foreign key constraints
ALTER TABLE mdm.etl_case_study_data
ADD CONSTRAINT fk_company_datasource_id
    FOREIGN KEY (company_datasource_id)
    REFERENCES mdm.etl_company_datasource (company_datasource_id);
"""

downgrade_sql = """
-- Truncate entire table
TRUNCATE TABLE mdm.etl_company_datasource_job;

-- remove column
ALTER TABLE mdm.etl_company_datasource_job
DROP COLUMN nlp_type,
DROP COLUMN urls,
DROP COLUMN status,
DROP COLUMN all_request_ids,
ADD COLUMN url varchar;

-- drop foreign key constraint in mdm.etl_case_study_data
ALTER TABLE mdm.etl_case_study_data 
DROP CONSTRAINT IF EXISTS fk_company_datasource_id;

-- truncate table etl_company_datasource
TRUNCATE TABLE mdm.etl_company_datasource;
-- insert back data from backup table
INSERT INTO mdm.etl_company_datasource
(
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    data_version_job,
    progress,
    payload,
    urls,
    nlp_type,
    status,
    all_request_ids
)
SELECT 
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    created_at,
    updated_at,
    data_version,
    data_version_job,
    progress,
    payload,
    urls,
    nlp_type,
    status,
    all_request_ids
FROM mdm.etl_company_datasource_bk;

-- re-add foreign key constraints
ALTER TABLE mdm.etl_case_study_data
ADD CONSTRAINT fk_company_datasource_id
    FOREIGN KEY (company_datasource_id)
    REFERENCES mdm.etl_company_datasource (company_datasource_id);
"""