"""add_datasource_tracking

Revision ID: a5f95c9aa338
Revises: f66e10220ad6
Create Date: 2021-01-26 11:09:18.364072

"""
from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = 'a5f95c9aa338'
down_revision = 'f66e10220ad6'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(CREATE_SQL)
    connection.commit()
    cur.close()
    connection.close()


def downgrade():
    pass


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mdm.etl_datasource (
	source_name varchar NULL,
	source_url varchar NULL,
	provider varchar NULL,
	source_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
	created_at timestamptz(0) NULL,
	updated_at timestamptz(0) NULL
);

ALTER TABLE mdm.etl_datasource ADD IF NOT EXISTS provider_job_id varchar NULL;
ALTER TABLE mdm.etl_datasource ADD IF NOT EXISTS batch_id integer NULL;
ALTER TABLE ONLY mdm.etl_datasource ADD CONSTRAINT etl_datasource_pkey PRIMARY KEY (source_id);
"""
