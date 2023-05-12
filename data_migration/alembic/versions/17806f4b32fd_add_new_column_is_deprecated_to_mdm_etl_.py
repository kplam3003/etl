"""add new column is_deprecated to mdm.etl_postprocess

Revision ID: 17806f4b32fd
Revises: 6d1d096ff767
Create Date: 2021-07-29 10:32:24.378658

"""
from alembic import op
import sqlalchemy as sa

import sys
sys.path.append('../')

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '17806f4b32fd'
down_revision = '6d1d096ff767'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(upgrade_sql)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Added is_deprecated column to mdm.etl_postprocess successfully!")


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
-- split into steps for faster operation
-- add column
ALTER TABLE mdm.etl_postprocess
    ADD COLUMN is_deprecated    BOOLEAN;
-- set current records' value to TRUE
UPDATE mdm.etl_postprocess
    SET is_deprecated = TRUE
    WHERE 1 = 1;
-- update default value to FALSE
ALTER TABLE mdm.etl_postprocess
    ALTER COLUMN is_deprecated SET DEFAULT FALSE;
"""

downgrade_sql = """
-- just drop the entire column
ALTER TABLE mdm.etl_case_study_data
    DROP COLUMN is_deprecated,
"""
