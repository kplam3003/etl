"""add column data_type to etl_case_study_data

Revision ID: acc042071b3d
Revises: d1c73e59713c
Create Date: 2021-11-10 12:11:48.063922

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'acc042071b3d'
down_revision = 'd1c73e59713c'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Insert column data_type to mdm.etl_case_study_data created successfully!")


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
-- add new column
ALTER TABLE mdm.etl_case_study_data
ADD COLUMN data_type character varying(255);

-- update column to fill legacy values
-- update for VoC
UPDATE mdm.etl_case_study_data er
SET data_type = 'review'
WHERE nlp_type = 'VoC';

-- update for VoE
UPDATE mdm.etl_case_study_data er
SET data_type = 'review'
WHERE nlp_type = 'VoE';

-- duplicate old entries and assign data_type 'job'
INSERT INTO mdm.etl_case_study_data
(
    case_study_id, 
    request_id, 
    company_datasource_id, 
    data_version, 
    nlp_type,
    nlp_pack,
    re_scrape,
    re_nlp,
    progress,
    payload,
    created_at,
    updated_at,
    status,
    data_version_job,
    data_type
)
SELECT
    case_study_id, 
    request_id, 
    company_datasource_id, 
    data_version, 
    nlp_type,
    nlp_pack,
    re_scrape,
    re_nlp,
    progress,
    payload,
    created_at,
    updated_at,
    status,
    data_version_job,
    'job' as data_type
FROM mdm.etl_case_study_data ecsd 
WHERE nlp_type = 'VoE';
"""

downgrade_sql = """
-- delete newly added rows
DELETE FROM mdm.etl_case_study_data
WHERE data_type = 'job';

-- delete the newly added column
ALTER TABLE mdm.etl_case_study_data
DROP COLUMN data_type;
"""

