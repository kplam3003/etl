"""hra - case study - update location

Revision ID: 9edbd7c29fca
Revises: 1cfb1ee95e80
Create Date: 2022-10-31 11:28:24.646717

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9edbd7c29fca"
down_revision = "1cfb1ee95e80"
branch_labels = None
depends_on = None


from google.cloud import bigquery
import config


project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART
employee_profiles_view_id = f"{project}.{datamart}.HRA_employee_profiles"
employee_experiences_view_id = f"{project}.{datamart}.HRA_employee_experiences"
hra_summary_table_employees_prefix = (
    f"{project}.{datamart_cs}.hra_summary_table_employees"
)
hra_summary_table_experience_prefix = (
    f"{project}.{datamart_cs}.hra_summary_table_experience"
)

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)


def upgrade():
    # get all current case_study_id
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`   
    """
    query_results = bqclient.query(query_cs_ids).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    # alter employees tables (for existing case studies)
    query_alter_table = """
        ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_employees_{case_study_id}`
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
    for case_study_id in case_study_list:
        _ = bqclient.query(
            query_alter_table.format(
                project=project,
                datamart_cs=datamart_cs,
                case_study_id=case_study_id,
            )
        ).result()

    # alter experience tables (for existing case studies)
    query_alter_table = """
        ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_experience_{case_study_id}`
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
    for case_study_id in case_study_list:
        _ = bqclient.query(
            query_alter_table.format(
                project=project,
                datamart_cs=datamart_cs,
                case_study_id=case_study_id,
            )
        ).result()

    # alter profile view (for csv export)
    query_create_view = f"""
        CREATE OR REPLACE VIEW {employee_profiles_view_id} AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            coresignal_employee_id AS employee_id,
            source_name,
            company_name,
            name,
            title
            url,
            industry,
            created,
            last_updated,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            country,
            connection_count
        FROM `{hra_summary_table_employees_prefix}_*`;
    """
    query_job = bqclient.query(query_create_view)
    query_job.result()

    # alter experience view (for csv export)
    query_create_view = f"""
        CREATE OR REPLACE VIEW {employee_experiences_view_id} AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            date_from,
            date_to,
            experience_id,
            coresignal_employee_id AS employee_id,
            job_function_group,
            job_function,
            most_similar_keyword AS keyword,
            title,
            company_name,
            company_id,
            coresignal_company_id,
            coresignal_company_name,
            coresignal_company_url,
            duration,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver
        FROM `{hra_summary_table_experience_prefix}_*`;
    """
    query_job = bqclient.query(query_create_view)
    query_job.result()

    print(f"<hra - case study - update location> successfully!")


def downgrade():
    # get all current case_study_id
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`   
    """
    query_results = bqclient.query(query_cs_ids).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    # alter experience tables (for existing case studies)
    query_alter_table = """
        ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_experience_{case_study_id}`
        DROP COLUMN IF EXISTS google_country,
        DROP COLUMN IF EXISTS google_admin1,
        DROP COLUMN IF EXISTS google_address_components;
    """
    for case_study_id in case_study_list:
        _ = bqclient.query(
            query_alter_table.format(
                project=project,
                datamart_cs=datamart_cs,
                case_study_id=case_study_id,
            )
        ).result()

    # alter experience view (for csv export)
    query_create_view = f"""
        CREATE OR REPLACE VIEW {employee_experiences_view_id} AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            date_from,
            date_to,
            experience_id,
            coresignal_employee_id AS employee_id,
            job_function_group,
            job_function,
            most_similar_keyword AS keyword,
            title,
            company_name,
            company_id,
            coresignal_company_id,
            coresignal_company_name,
            coresignal_company_url,
            duration,
            google_location,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver
        FROM `{hra_summary_table_experience_prefix}_*`;
    """
    query_job = bqclient.query(query_create_view)
    query_job.result()

    print(f"Rollback l<hra - case study - update location> successfully!")
