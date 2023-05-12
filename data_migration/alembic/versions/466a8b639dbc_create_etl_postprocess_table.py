"""create_etl_postprocess_table

Revision ID: 466a8b639dbc
Revises: 3456fcf0cb42
Create Date: 2021-04-05 04:31:46.672982

"""

import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa

from utils import open_etl_database_connection

# revision identifiers, used by Alembic.
revision = '466a8b639dbc'
down_revision = '3456fcf0cb42'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(query_SQL1)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Create table etl_postprocess successfull!")


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(query_SQL2)
    connection.commit()
    cur.close()
    connection.close()
    print("\n Downgrade 3456fcf0cb42 process is successfull!")
	
query_SQL1 = """
CREATE TABLE mdm.etl_postprocess (
    request_id integer,
	case_study_id integer, 
    status character varying(255),
    type character varying(255),
    meta_data jsonb,
    created_at timestamp without time zone,
	updated_at timestamp without time zone
);
"""

query_SQL2 = """
DROP TABLE mdm.etl_postprocess;
"""