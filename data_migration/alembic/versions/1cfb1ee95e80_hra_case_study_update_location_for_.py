"""hra - case study - update location

Revision ID: 1cfb1ee95e80
Revises: b749f2d84505
Create Date: 2022-10-27 16:11:05.721307

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1cfb1ee95e80'
down_revision = 'b749f2d84505'
branch_labels = None
depends_on = None


from google.cloud import bigquery
import config

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH


def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    upgrade_scripts = f"""
        ALTER TABLE `{project}.{staging}.coresignal_employees`
        ADD COLUMN IF NOT EXISTS google_country STRING,
        ADD COLUMN IF NOT EXISTS google_admin1 STRING,
        ADD COLUMN IF NOT EXISTS google_address_components ARRAY<
            STRUCT<
                long_name STRING,
                short_name STRING,
                types ARRAY<STRING>
            >
        >;

        ALTER TABLE `{project}.{staging}.coresignal_employees_experiences`
        ADD COLUMN IF NOT EXISTS google_country STRING,
        ADD COLUMN IF NOT EXISTS google_admin1 STRING,
        ADD COLUMN IF NOT EXISTS google_address_components ARRAY<
            STRUCT<
                long_name STRING,
                short_name STRING,
                types ARRAY<STRING>
            >
        >;
    """
    query_job = bqclient.query(upgrade_scripts)
    query_job.result()
    print(f"<hra - case study - update location> successfully!")


def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    downgrade_scripts = f"""
        ALTER TABLE `{project}.{staging}.coresignal_employees`
        DROP COLUMN IF EXISTS google_country,
        DROP COLUMN IF EXISTS google_admin1,
        DROP COLUMN IF EXISTS google_address_components;

        ALTER TABLE `{project}.{staging}.coresignal_employees_experiences`
        DROP COLUMN IF EXISTS google_country,
        DROP COLUMN IF EXISTS google_admin1,
        DROP COLUMN IF EXISTS google_address_components;
    """
    query_job = bqclient.query(downgrade_scripts)
    query_job.result()
    print(f"Rollback <hra - case study - update location> successfully!")
