"""Create mdm.etl_case_study

Revision ID: 323ae6d11529
Revises: 49818487ece2
Create Date: 2021-07-23 18:01:31.326512

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '323ae6d11529'
down_revision = '49818487ece2'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Table mdm.etl_case_study created successfully!")


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
CREATE TABLE IF NOT EXISTS mdm.etl_case_study (
    case_study_id			integer PRIMARY KEY,
    request_id				integer not null,
    case_study_name         varchar(255),
    nlp_type                varchar(100),
    nlp_pack                varchar(200),
    created_at				timestamp,
    updated_at				timestamp,
    status					varchar(100),
    progress				real,
    payload					jsonb,
    CONSTRAINT fk_request_id
        FOREIGN KEY(request_id) 
        REFERENCES mdm.etl_request(request_id)
);
"""

downgrade_sql = """
DROP TABLE IF EXISTS mdm.etl_case_study;
"""