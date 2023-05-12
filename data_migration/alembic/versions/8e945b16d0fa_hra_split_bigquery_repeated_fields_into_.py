"""hra - split bigquery repeated fields into tables

Revision ID: 8e945b16d0fa
Revises: 9a2f2099dade
Create Date: 2022-10-19 17:54:50.844366

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8e945b16d0fa"
down_revision = "9a2f2099dade"
branch_labels = None
depends_on = None


from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference
import config
import pymongo
from pymongo import MongoClient

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH


def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

    # Prepare data for `company_url`
    query_job = bqclient.query(
        f"""
        CREATE OR REPLACE table `{project}.{staging}.tmp_company_url` (
            `coresignal_employee_id` int64,
            `experience_id` int64,
            `company_url` string
        );
    """
    )
    query_job.result()

    mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)
    curr = database[config.MONGODB_CORESIGNAL_EMPLOYEES].aggregate(
        [
            {"$match": {"member_experience_collection": {"$ne": []}}},
            {
                "$project": {
                    "_id": 0,
                    "coresignal_employee_id": "$coresignal_id",
                    "member_experience_collection": 1,
                }
            },
            {
                "$unwind": {
                    "path": "$member_experience_collection",
                    "preserveNullAndEmptyArrays": False,
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            {"coresignal_employee_id": "$coresignal_employee_id"},
                            "$member_experience_collection",
                        ]
                    }
                }
            },
            {
                "$project": {
                    "coresignal_employee_id": 1,
                    "experience_id": "$id",
                    "company_url": 1,
                }
            },
        ]
    )

    tb_ref = TableReference.from_string(
        table_id=f"{project}.{staging}.tmp_company_url",
        default_project=config.GCP_PROJECT_ID,
    )
    table = bqclient.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bqclient.load_table_from_json(
        destination=tb_ref,
        json_rows=list(curr),
        job_config=job_config,
    )
    _ = load_job.result()

    # Apply changes to BigQuery
    upgrade_scripts = f"""
        CREATE OR REPLACE table `{project}.{staging}.coresignal_employees_experiences` (
            `coresignal_employee_id` int64,
            `case_study_id` int64,
            `company_datasource_id` int64,
            `experience_id` int64,
            `title` string,
            `coresignal_company_id` int64,
            `company_name` string,
            `company_url` string,
            `date_from` date,
            `date_to` date,
            `duration` string,
            `previous_coresignal_company_id` int64,
            `previous_company_name` string,
            `previous_experience_id` int64,
            `next_coresignal_company_id` int64,
            `next_company_name` string,
            `next_experience_id` int64,
            `google_location` string,
            `is_starter` bool,
            `is_leaver` bool
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

        CREATE OR REPLACE table `{project}.{staging}.coresignal_employees_education` (
            `coresignal_employee_id` int64,
            `case_study_id` int64,
            `company_datasource_id` int64,
            `education_id` int64,
            `title` string,
            `subtitle` string,
            `description` string,
            `date_from` date,
            `date_to` date
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

        INSERT INTO `{project}.{staging}.coresignal_employees_experiences`
        SELECT ce.coresignal_employee_id,
            ce.case_study_id,
            ce.company_datasource_id,
            mec.id as experience_id,
            mec.title,
            mec.coresignal_company_id,
            mec.company_name,
            cu.company_url,
            mec.date_from,
            mec.date_to,
            mec.duration,
            mec.previous_coresignal_company_id,
            mec.previous_company_name,
            mec.previous_experience_id,
            mec.next_coresignal_company_id,
            mec.next_company_name,
            mec.next_experience_id,
            mec.google_location,
            mec.is_starter,
            mec.is_leaver
        FROM `{project}.{staging}.coresignal_employees` AS ce
            CROSS JOIN UNNEST(ce.member_experience_collection) AS mec
            INNER JOIN `{project}.{staging}.tmp_company_url` as cu
            ON ce.coresignal_employee_id = cu.coresignal_employee_id AND mec.id = cu.experience_id;

        INSERT INTO `{project}.{staging}.coresignal_employees_education`
        SELECT ce.coresignal_employee_id,
            ce.case_study_id,
            ce.company_datasource_id,
            mec.id as education_id,
            mec.title,
            mec.subtitle,
            mec.description,
            mec.date_from,
            mec.date_to
        FROM `{project}.{staging}.coresignal_employees` AS ce
            CROSS JOIN UNNEST(ce.member_education_collection) AS mec;

        ALTER TABLE `{project}.{staging}.coresignal_employees`
        DROP COLUMN IF EXISTS member_experience_collection;

        ALTER TABLE `{project}.{staging}.coresignal_employees`
        DROP COLUMN IF EXISTS member_education_collection;

        DROP TABLE IF EXISTS `{project}.{staging}.tmp_company_url`;
    """
    query_job = bqclient.query(upgrade_scripts)
    query_job.result()
    print(f"<hra - split bigquery repeated fields into tables> successfully!")


def downgrade():
    downgrade_scripts = f"""
        DROP TABLE IF EXISTS `{project}.{staging}.coresignal_employees_experiences`;
        DROP TABLE IF EXISTS `{project}.{staging}.coresignal_employees_education`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(downgrade_scripts)
    query_job.result()
    print(f"Rollback <hra - split bigquery repeated fields into tables> successfully!")
