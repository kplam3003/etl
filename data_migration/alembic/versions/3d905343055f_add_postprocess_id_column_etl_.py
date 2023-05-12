"""add_postprocess_id_column_etl_postprocess

Revision ID: 3d905343055f
Revises: 3c3f92ddaacd
Create Date: 2021-04-05 08:53:22.806111

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa

from utils import open_etl_database_connection
# revision identifiers, used by Alembic.
revision = '3d905343055f'
down_revision = '3c3f92ddaacd'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Add column postprocess_id and progress columns in table etl_postprocess successfull!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade 3d905343055f process is successfull!")

SQL_upgrade="""
ALTER TABLE mdm.etl_postprocess
ADD COLUMN progress double precision,
ADD COLUMN postprocess_type character varying(255),
ADD COLUMN postprocess_error character varying(255),
ADD COLUMN postprocess_id integer NOT NULL GENERATED ALWAYS AS IDENTITY;

ALTER TABLE mdm.etl_postprocess
ADD CONSTRAINT etl_postprocess_pkey PRIMARY KEY (postprocess_id);

ALTER TABLE mdm.etl_postprocess
DROP COLUMN type;
"""


SQL_downgrade="""
ALTER TABLE mdm.etl_postprocess
DROP COLUMN progress,
DROP COLUMN postprocess_type,
DROP COLUMN postprocess_error,
DROP COLUMN postprocess_id;

ALTER TABLE mdm.etl_postprocess
ADD COLUMN type character varying(255);

"""

