"""add error_message and is_translate

Revision ID: 7905558b2714
Revises: aea5a209252e
Create Date: 2021-01-14 09:58:15.373667

"""
import sys
sys.path.append('../')

from alembic import op
import sqlalchemy as sa
from utils import open_etl_database_connection


# revision identifiers, used by Alembic.
revision = '7905558b2714'
down_revision = 'aea5a209252e'
branch_labels = None
depends_on = None


def upgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_upgrade)
    connection.commit()
    cur.close()
    connection.close()


def downgrade():
    connection = open_etl_database_connection()
    cur = connection.cursor()
    cur.execute(SQL_downgrade)
    connection.commit()
    cur.close()
    connection.close()

SQL_upgrade="""
ALTER TABLE mdm.etl_company_batch 
ADD column error_message varchar,
ADD column is_translation bool
;

ALTER TABLE mdm.etl_step 
ADD column error_message varchar,
ADD column is_translation bool;

ALTER TABLE mdm.etl_request 
ADD column error_message varchar,
ADD column is_translation bool;

ALTER TABLE mdm.etl_step_detail 
ADD column error_message varchar,
ADD column is_translation bool;


CREATE TABLE mdm.etl_operation
(
    operation_id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    request_id integer,
    operation character varying(50) COLLATE pg_catalog."default",
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    CONSTRAINT etl_operation_pkey PRIMARY KEY (operation_id),
    CONSTRAINT request_id_index UNIQUE (request_id)
);

CREATE INDEX operation_index
    ON mdm.etl_operation USING btree
    (operation COLLATE pg_catalog."default" ASC NULLS LAST);


"""


SQL_downgrade="""
ALTER TABLE mdm.etl_company_batch 
DROP column error_message,
DROP column is_translation
;

ALTER TABLE mdm.etl_step 
DROP column error_message,
DROP column is_translation
;

ALTER TABLE mdm.etl_request 
DROP column error_message,
DROP column is_translation
;

ALTER TABLE mdm.etl_step_detail 
DROP column error_message,
DROP column is_translation
;

DROP TABLE mdm.etl_operation;

"""
