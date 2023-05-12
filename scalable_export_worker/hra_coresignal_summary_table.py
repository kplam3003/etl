import sys

sys.path.append("../")

from google.cloud import bigquery

import config
from core import logger
from hra_coresignal_turnover import (
    build_query_company,
    build_query_company_country,
    build_query_company_job_function,
    build_query_company_tenure_bucket
)

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def delete_summary_table_hra(
    bqclient,
    case_study_id,
    table_summary_table_prefix,
):
    logger.info(f"[MOVING_DATA] delete {table_summary_table_prefix}: {case_study_id}")
    table_id = None
    try:
        table_id = f"{table_summary_table_prefix}_{case_study_id}"
        bqclient.delete_table(table_id, not_found_ok=True)
        return True
    except Exception as error:
        logger.exception(
            "[MOVING_DATA] delete {}: case_study_id={},error={},table={}".format(
                table_summary_table_prefix,
                case_study_id,
                error.__class__.__name__,
                table_id,
            )
        )
        raise error


def create_hra_summary_table_experience(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EXPERIENCE_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- Job classification result
            experience_id INTEGER,
            title STRING,
            job_function_group STRING,
            modified_job_function_group STRING,
            job_function STRING,
            modified_job_function STRING,
            is_used BOOLEAN,
            most_similar_keyword STRING,
            
            -- Experience information
            experience_company_id INTEGER,
            coresignal_employee_id INTEGER,
            coresignal_company_id INTEGER,
            coresignal_company_name STRING,
            coresignal_company_url STRING,
            date_from DATE,
            date_to DATE,
            duration STRING,
            previous_coresignal_company_id INTEGER,
            previous_company_name STRING,
            previous_experience_id INTEGER,
            next_coresignal_company_id INTEGER,
            next_company_name STRING,
            next_experience_id INTEGER,
            google_country STRING,
            google_admin1 STRING,
            google_address_components ARRAY<
                STRUCT<
                    long_name STRING,
                    short_name STRING,
                    types ARRAY<STRING>
                >
            >,
            is_starter BOOLEAN,
            is_leaver BOOLEAN,

            is_target BOOLEAN,
            is_cohort BOOLEAN,

            workforce_years FLOAT64,
            company_years FLOAT64,
            role_years FLOAT64,
            last_education_years FLOAT64
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_experience(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_experience_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EXPERIENCE_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}`
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name,
                coresignal_company_id
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        experience_data AS (
            SELECT
                ce.case_study_id,
                ce.case_study_name,
                ce.company_datasource_id,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                ecd.company_id as experience_company_id,
                ce.url,
                ce.coresignal_employee_id,
                mec.experience_id AS experience_id,
                mec.title,
                mec.coresignal_company_id,
                mec.company_name AS coresignal_company_name,
                mec.company_url AS coresignal_company_url,
                mec.date_from,
                mec.date_to,
                mec.duration,
                mec.previous_coresignal_company_id,
                mec.previous_company_name,
                mec.previous_experience_id,
                mec.next_coresignal_company_id,
                mec.next_company_name,
                mec.next_experience_id,
                mec.google_country,
                mec.google_admin1,
                mec.google_address_components,
                mec.is_starter,
                mec.is_leaver,
                mec.is_target,
                mec.is_cohort
            FROM `{table_coresignal_employees}` ce
            LEFT JOIN `{table_coresignal_employees_experiences}` mec
                ON ce.company_datasource_id = mec.company_datasource_id
                AND ce.coresignal_employee_id = mec.coresignal_employee_id
            LEFT JOIN company_datasource cd
                ON ce.company_datasource_id = cd.company_datasource_id
            LEFT JOIN company_datasource ecd
                ON mec.coresignal_company_id = ecd.coresignal_company_id
            WHERE ce.case_study_id = @case_study_id
                AND mec.case_study_id = @case_study_id
        ),
        classification AS (
            SELECT DISTINCT
                company_datasource_id,
                experience_id,
                job_function_group,
                job_function,
                most_similar_keyword
            FROM `{table_hra_experience_function}`
            WHERE case_study_id = @case_study_id
                AND run_id = @run_id
        ),
        dimension_config AS (
            SELECT DISTINCT
                cdf.case_study_id,
                cdf.dimension_config_id,
                cdf.dimension_config_name,
                cdf.nlp_pack,
                d.dimension_type,
                d.is_used,
                d.is_user_defined,
                d.job_function_group,
                d.modified_job_function_group,
                d.job_function,
                d.modified_job_function
            FROM `{table_hra_casestudy_dimension_config}` cdf,
                UNNEST(cdf.dimensions) d
            WHERE case_study_id = @case_study_id
                AND run_id = @run_id
        ),
        tenure_data AS (
            SELECT DISTINCT
                et.company_datasource_id,
                et.employee_id,
                et.experience_id,
                et.workforce_years,
                et.company_years,
                et.role_years,
                et.last_education_years
            FROM `{table_hra_employees_tenure}` et
            WHERE case_study_id = @case_study_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            ed.case_study_id,
            ed.case_study_name,
            ed.company_datasource_id,
            ed.source_id,
            ed.source_name,
            ed.company_id,
            ed.company_name,
            @run_id AS run_id,

            -- Dimension info
            dc.nlp_pack,
            'HRA' AS nlp_type,
            dc.dimension_config_id,
            dc.dimension_config_name,

            -- Job classification result
            ed.experience_id,
            ed.title,
            cls.job_function_group,
            dc.modified_job_function_group,
            cls.job_function,
            dc.modified_job_function,
            dc.is_used,
            cls.most_similar_keyword,

            -- Experience information
            ed.experience_company_id,
            ed.coresignal_employee_id,
            ed.coresignal_company_id,
            ed.coresignal_company_name,
            ed.coresignal_company_url,
            ed.date_from,
            ed.date_to,
            ed.duration,
            ed.previous_coresignal_company_id,
            ed.previous_company_name,
            ed.previous_experience_id,
            ed.next_coresignal_company_id,
            ed.next_company_name,
            ed.next_experience_id,
            ed.google_country,
            ed.google_admin1,
            ed.google_address_components,
            ed.is_starter,
            ed.is_leaver,

            ed.is_target,
            ed.is_cohort,

            td.workforce_years,
            td.company_years,
            td.role_years,
            td.last_education_years
        FROM experience_data ed
        LEFT JOIN classification cls
            ON ed.company_datasource_id = cls.company_datasource_id
            AND ed.experience_id = cls.experience_id
        LEFT JOIN dimension_config dc
            ON cls.job_function_group = dc.job_function_group
            AND cls.job_function = dc.job_function
        LEFT JOIN tenure_data td
            ON ed.coresignal_employee_id = td.employee_id
            AND ed.company_datasource_id = td.company_datasource_id
            AND ed.experience_id = td.experience_id
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_employees=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
            table_coresignal_employees_experiences=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES,
            table_hra_experience_function=config.GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION,
            table_hra_casestudy_dimension_config=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
            table_hra_employees_tenure=config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert {}_{} ,error={}, table={}".format(
                config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX,
                case_study_id,
                error.__class__.__name__,
                summary_table_id,
            )
        )
        raise error


def create_hra_summary_table_education(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- Education information
            education_id INTEGER,
            coresignal_employee_id INTEGER,
            title STRING,
            subtitle STRING,
            description STRING,
            date_from DATE,
            date_to DATE,
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_education(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_education_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}` 
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        education_data AS (
            SELECT
                ce.case_study_id,
                ce.case_study_name,
                ce.company_datasource_id,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                ce.url,
                ce.coresignal_employee_id,
                mec.education_id,
                mec.title,
                mec.subtitle,
                mec.description,
                mec.date_from,
                mec.date_to
            FROM `{table_coresignal_employees}` ce
            LEFT JOIN `{table_coresignal_education}` mec
                ON ce.company_datasource_id = mec.company_datasource_id
                AND ce.coresignal_employee_id = mec.coresignal_employee_id
            LEFT JOIN company_datasource cd
                ON ce.company_datasource_id = cd.company_datasource_id
            WHERE ce.case_study_id = @case_study_id
                AND mec.case_study_id = @case_study_id
        ),
        dimension_config AS (
            SELECT DISTINCT 
                cdf.case_study_id,
                cdf.dimension_config_id,
                cdf.dimension_config_name,
                cdf.nlp_pack,
            FROM `{table_hra_casestudy_dimension_config}` cdf
            WHERE case_study_id = @case_study_id
                AND run_id = @run_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            ed.case_study_id,
            ed.case_study_name,
            ed.company_datasource_id,
            ed.source_id,
            ed.source_name,
            ed.company_id,
            ed.company_name,
            @run_id AS run_id,

            -- Dimension info
            dc.nlp_pack,
            'HRA' AS nlp_type,
            dc.dimension_config_id,
            dc.dimension_config_name,

            -- Education information
            ed.education_id,
            ed.coresignal_employee_id,
            ed.title,
            ed.subtitle,
            ed.description,
            ed.date_from,
            ed.date_to
        FROM education_data ed
        LEFT JOIN dimension_config dc
            ON ed.case_study_id = dc.case_study_id
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_employees=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
            table_coresignal_education=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION,
            table_hra_experience_function=config.GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION,
            table_hra_casestudy_dimension_config=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert {}_{} ,error={}, table={}".format(
                config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX,
                case_study_id,
                error.__class__.__name__,
                summary_table_id,
            )
        )
        raise error


def create_hra_summary_table_education_degree(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_DEGREE_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,

            -- Education info
            education_id INTEGER,
            coresignal_employee_id INTEGER,
            title STRING,
            subtitle STRING,
            description STRING,
            date_from DATE,
            date_to DATE,
            school_url STRING,

            -- Education degree
            degree_level STRING,
            degree_level_code INTEGER,
            degree_confidence FLOAT64
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_education_degree(bqclient, case_study_id, run_id, edu_dim_conf):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_education_degree_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_DEGREE_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}` 
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        education_data AS (
            SELECT
                emp.case_study_id,
                emp.case_study_name,
                emp.company_datasource_id,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                emp.url,
                emp.coresignal_employee_id,
                edu.education_id,
                edu.title,
                edu.subtitle,
                edu.description,
                edu.date_from,
                edu.date_to,
                edu.school_url,
                deg.level,
                deg.level_code,
                deg.confidence
            FROM `{table_coresignal_employees}` emp
            LEFT JOIN `{table_coresignal_education}` edu
                ON emp.company_datasource_id = edu.company_datasource_id
                AND emp.coresignal_employee_id = edu.coresignal_employee_id
            LEFT JOIN company_datasource cd
                ON emp.company_datasource_id = cd.company_datasource_id
            LEFT JOIN `{table_hra_education_degree}` deg
                ON emp.company_datasource_id = deg.company_datasource_id
                AND edu.education_id = deg.education_id
            WHERE emp.case_study_id = @case_study_id
                AND edu.case_study_id = @case_study_id
                AND deg.case_study_id = @case_study_id
                AND deg.run_id = @run_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            ed.case_study_id,
            ed.case_study_name,
            ed.company_datasource_id,
            ed.source_id,
            ed.source_name,
            ed.company_id,
            ed.company_name,
            @run_id AS run_id,

            -- Dimension info
            '{dim_nlp_pack}' AS nlp_pack,
            '{dim_nlp_type}' AS nlp_type,
            {dim_config_id} AS dimension_config_id,
            '{dim_config_name}' AS dimension_config_name,

            -- Education info
            ed.education_id,
            ed.coresignal_employee_id,
            ed.title,
            ed.subtitle,
            ed.description,
            ed.date_from,
            ed.date_to,
            ed.school_url,

            -- Education degree
            ed.level,
            ed.level_code,
            ed.confidence
        FROM education_data ed
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_employees=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
            table_coresignal_education=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION,
            table_hra_education_degree=config.GCP_BQ_TABLE_HRA_EDUCATION_DEGREE,
            dim_nlp_pack=edu_dim_conf["nlp_pack"],
            dim_nlp_type=edu_dim_conf["nlp_type"],
            dim_config_id=edu_dim_conf["id"],
            dim_config_name=edu_dim_conf["name"],
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert {}_{} ,error={}, table={}".format(
                config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EDUCATION_DEGREE_PREFIX,
                case_study_id,
                error.__class__.__name__,
                summary_table_id,
            )
        )
        raise error


def create_hra_summary_table_employees(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            url STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- employee info result
            coresignal_employee_id INTEGER,
            name STRING,
            title STRING,
            industry STRING,
            created TIMESTAMP,
            last_updated TIMESTAMP,
            country STRING,
            google_country STRING,
            google_admin1 STRING,
            google_address_components ARRAY<
                STRUCT<
                    long_name STRING,
                    short_name STRING,
                    types ARRAY<STRING>
                >
            >,
            connection_count INTEGER,
            num_experience_records INTEGER,
            num_education_records INTEGER,
            highest_education_level_code INTEGER,
            highest_education_level STRING
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] create_summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_employees(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_employees_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}` 
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        experience_data AS (
            SELECT 
                coresignal_employee_id,
                COUNT(experience_id) AS num_experience_records,
            FROM `{table_coresignal_employees_experiences}`
            WHERE case_study_id = @case_study_id
            GROUP BY coresignal_employee_id
        ),
        education_data AS (
            SELECT 
                coresignal_employee_id,
                COUNT(education_id) AS num_education_records,
            FROM `{table_coresignal_employees_education}`
            WHERE case_study_id = @case_study_id
            GROUP BY coresignal_employee_id
        ),
        edu_degree_data AS (
            SELECT ced.coresignal_employee_id,
                ed.level,
                ed.level_code,
                ROW_NUMBER() OVER(PARTITION BY ced.coresignal_employee_id ORDER BY ed.level_code ASC) as edu_degree_rank
            FROM `{table_coresignal_employees_education}` ced
                INNER JOIN `{table_hra_education_degree}` ed
                ON ced.education_id = ed.education_id
            WHERE ced.case_study_id = @case_study_id
                AND ed.case_study_id = @case_study_id
                AND ed.run_id = @run_id
        ),
        employee_data AS (
            SELECT
                ce.case_study_id,
                ce.case_study_name,
                ce.company_datasource_id,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                ce.url,
                ce.coresignal_employee_id,
                ce.name,
                ce.title,
                ce.industry,
                ce.created,
                ce.last_updated,
                ce.country,
                ce.google_country,
                ce.google_admin1,
                ce.google_address_components,
                ce.connection_count,
                expd.num_experience_records,
                edud.num_education_records
            FROM `{table_coresignal_employees}` ce
            LEFT JOIN company_datasource cd
                ON ce.case_study_id = cd.case_study_id
                AND ce.company_datasource_id = cd.company_datasource_id
            LEFT JOIN experience_data expd
                ON ce.coresignal_employee_id = expd.coresignal_employee_id
            LEFT JOIN education_data edud
                ON ce.coresignal_employee_id = edud.coresignal_employee_id
            WHERE ce.case_study_id = @case_study_id
        ),
        dimension_config AS (
            SELECT DISTINCT 
                case_study_id,
                dimension_config_id,
                dimension_config_name,
                nlp_pack
            FROM `{table_hra_casestudy_dimension_config}`
            WHERE case_study_id = @case_study_id
            AND run_id = @run_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            ed.case_study_id,
            ed.case_study_name,
            ed.company_datasource_id,
            ed.source_id,
            ed.source_name,
            ed.company_id,
            ed.company_name,
            ed.url,
            @run_id AS run_id,

            -- Dimension info
            dc.nlp_pack,
            'HRA' AS nlp_type,
            dc.dimension_config_id,
            dc.dimension_config_name,

            -- employee info result
            ed.coresignal_employee_id,
            ed.name,
            ed.title,
            ed.industry,
            ed.created,
            ed.last_updated,
            ed.country,
            ed.google_country,
            ed.google_admin1,
            ed.google_address_components,
            ed.connection_count,
            ed.num_experience_records,
            ed.num_education_records,
            edd.level_code as highest_education_level_code,
            edd.level as highest_education_level
        FROM employee_data ed
        LEFT JOIN dimension_config dc
            ON ed.case_study_id = dc.case_study_id
        LEFT JOIN edu_degree_data edd
            ON ed.coresignal_employee_id = edd.coresignal_employee_id
            AND edd.edu_degree_rank = 1
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_employees=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
            table_hra_casestudy_dimension_config=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
            table_coresignal_employees_experiences=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES,
            table_coresignal_employees_education=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION,
            table_hra_education_degree=config.GCP_BQ_TABLE_HRA_EDUCATION_DEGREE,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert {}_{} ,error={}, table={}".format(
                config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_EMPLOYEES_PREFIX,
                case_study_id,
                error.__class__.__name__,
                summary_table_id,
            )
        )
        raise error


def create_hra_summary_table_company(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_COMPANY_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- company stats result
            coresignal_company_id INTEGER, 
            coresignal_url STRING, 
            name STRING, 
            website STRING, 
            size STRING, 
            industry STRING, 
            followers INTEGER, 
            founded INTEGER, 
            created TIMESTAMP, 
            last_updated TIMESTAMP, 
            type STRING, 
            employees_count INTEGER, 
            headquarters_country_parsed STRING, 
            company_shorthand_name STRING, 
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] create_summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


# NOTE: WIP
def insert_hra_summary_table_company(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_company_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_COMPANY_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}` 
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        company_data AS (
            SELECT
                cs.case_study_id,
                cs.case_study_name,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                cs.company_datasource_id,
                cs.coresignal_company_id,
                cs.url AS coresignal_url,
                cs.name,
                cs.website,
                cs.size, 
                cs.industry, 
                cs.followers, 
                cs.founded, 
                cs.created, 
                cs.last_updated, 
                cs.type, 
                cs.employees_count, 
                cs.headquarters_country_parsed, 
                cs.company_shorthand_name
            FROM `{table_coresignal_stats}` cs
            LEFT JOIN company_datasource cd
                ON cs.case_study_id = cd.case_study_id
                AND cs.company_datasource_id = cd.company_datasource_id
            WHERE cs.case_study_id = @case_study_id
        ),
        dimension_config AS (
            SELECT 
                case_study_id,
                dimension_config_id,
                dimension_config_name,
                nlp_pack
            FROM `{table_hra_casestudy_dimension_config}`
            WHERE case_study_id = @case_study_id
            AND run_id = @run_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            cda.case_study_id,
            cda.case_study_name,
            cda.company_datasource_id,
            cda.source_id,
            cda.source_name,
            cda.company_id,
            cda.company_name,
            @run_id AS run_id,

            -- Dimension info
            dc.nlp_pack,
            'HRA' AS nlp_type,
            dc.dimension_config_id,
            dc.dimension_config_name,

            -- company stats result
            coresignal_company_id, 
            coresignal_url, 
            name, 
            website, 
            size, 
            industry, 
            followers, 
            founded, 
            created, 
            last_updated, 
            type, 
            employees_count, 
            headquarters_country_parsed, 
            company_shorthand_name
        FROM company_data cda
        LEFT JOIN dimension_config dc
            ON cda.case_study_id = dc.case_study_id
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_stats=config.GCP_BQ_TABLE_CORESIGNAL_STATS,
            table_hra_casestudy_dimension_config=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, summary_table_id
            )
        )
        raise error


def create_hra_summary_table_monthly_dataset(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_MONTHLY_DATASET_PREFIX}_{case_study_id}"
        )
        query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- Job classification result
            experience_id INTEGER,
            title STRING,
            job_function_group STRING,
            modified_job_function_group STRING,
            job_function STRING,
            modified_job_function STRING,
            is_used BOOLEAN,
            most_similar_keyword STRING,
            
            -- Experience information
            experience_company_id INTEGER,
            coresignal_employee_id INTEGER,
            coresignal_company_id INTEGER,
            coresignal_company_name STRING,
            coresignal_company_url STRING,
            date_from DATE,
            date_to DATE,
            duration STRING,
            previous_coresignal_company_id INTEGER,
            previous_company_name STRING,
            previous_experience_id INTEGER,
            next_coresignal_company_id INTEGER,
            next_company_name STRING,
            next_experience_id INTEGER,
            google_country STRING,
            google_admin1 STRING,
            google_address_components ARRAY<
                STRUCT<
                    long_name STRING,
                    short_name STRING,
                    types ARRAY<STRING>
                >
            >,
            is_starter BOOLEAN,
            is_leaver BOOLEAN,

            is_target BOOLEAN,
            is_cohort BOOLEAN,
            
            -- Monthly dataset
            reference_date DATE,
            workforce_years FLOAT64,
            company_years FLOAT64,
            role_years FLOAT64,
            last_education_years FLOAT64
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] create_summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_monthly_dataset(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_monthly_dataset_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_MONTHLY_DATASET_PREFIX}_{case_study_id}"
        )
        query = """
        INSERT INTO `{summary_table_id}`
        WITH company_datasource AS (
            SELECT
                case_study_id,
                company_datasource_id,
                source_id,
                source_name,
                company_id,
                company_name,
                coresignal_company_id
            FROM `{table_coresignal_company_datasource}`
            WHERE case_study_id = @case_study_id
        ),
        experience_data AS (
            SELECT
                ce.case_study_id,
                ce.case_study_name,
                ce.company_datasource_id,
                cd.source_id,
                cd.source_name,
                cd.company_id,
                cd.company_name,
                ecd.company_id as experience_company_id,
                ce.url,
                ce.coresignal_employee_id,
                mec.experience_id AS experience_id,
                mec.title,
                mec.coresignal_company_id,
                mec.company_name AS coresignal_company_name,
                mec.company_url AS coresignal_company_url,
                mec.date_from,
                mec.date_to,
                mec.duration,
                mec.previous_coresignal_company_id,
                mec.previous_company_name,
                mec.previous_experience_id,
                mec.next_coresignal_company_id,
                mec.next_company_name,
                mec.next_experience_id,
                mec.google_country,
                mec.google_admin1,
                mec.google_address_components,
                mec.is_starter,
                mec.is_leaver,
                mec.is_target,
                mec.is_cohort
            FROM `{table_coresignal_employees}` ce
            LEFT JOIN `{table_coresignal_employees_experiences}` mec
                ON ce.company_datasource_id = mec.company_datasource_id
                AND ce.coresignal_employee_id = mec.coresignal_employee_id
            LEFT JOIN company_datasource cd
                ON ce.company_datasource_id = cd.company_datasource_id
            LEFT JOIN company_datasource ecd
                ON mec.coresignal_company_id = ecd.coresignal_company_id
            WHERE ce.case_study_id = @case_study_id
                AND mec.case_study_id = @case_study_id
        ),
        classification AS (
            SELECT DISTINCT
                company_datasource_id,
                experience_id,
                job_function_group,
                job_function,
                most_similar_keyword
            FROM `{table_hra_experience_function}`
            WHERE case_study_id = @case_study_id
                AND run_id = @run_id
        ),
        dimension_config AS (
            SELECT DISTINCT
                cdf.case_study_id,
                cdf.dimension_config_id,
                cdf.dimension_config_name,
                cdf.nlp_pack,
                d.dimension_type,
                d.is_used,
                d.is_user_defined,
                d.job_function_group,
                d.modified_job_function_group,
                d.job_function,
                d.modified_job_function
            FROM `{table_hra_casestudy_dimension_config}` cdf,
                UNNEST(cdf.dimensions) d
            WHERE case_study_id = @case_study_id
                AND run_id = @run_id
        ),
        monthly_tenure_data AS (
            SELECT DISTINCT
                company_datasource_id,
                employee_id,
                experience_id,
                reference_date,
                workforce_years,
                company_years,
                role_years,
                last_education_years
            FROM `{table_hra_employees_tenure_monthly}`
            WHERE case_study_id = @case_study_id
        )
        SELECT 
            CURRENT_TIMESTAMP() AS created_at,
            ed.case_study_id,
            ed.case_study_name,
            ed.company_datasource_id,
            ed.source_id,
            ed.source_name,
            ed.company_id,
            ed.company_name,
            @run_id AS run_id,

            -- Dimension info
            dc.nlp_pack,
            'HRA' AS nlp_type,
            dc.dimension_config_id,
            dc.dimension_config_name,

            -- Job classification result
            ed.experience_id,
            ed.title,
            cls.job_function_group,
            dc.modified_job_function_group,
            cls.job_function,
            dc.modified_job_function,
            dc.is_used,
            cls.most_similar_keyword,

            -- Experience information
            ed.experience_company_id,
            ed.coresignal_employee_id,
            ed.coresignal_company_id,
            ed.coresignal_company_name,
            ed.coresignal_company_url,
            ed.date_from,
            ed.date_to,
            ed.duration,
            ed.previous_coresignal_company_id,
            ed.previous_company_name,
            ed.previous_experience_id,
            ed.next_coresignal_company_id,
            ed.next_company_name,
            ed.next_experience_id,
            ed.google_country,
            ed.google_admin1,
            ed.google_address_components,
            ed.is_starter,
            ed.is_leaver,

            ed.is_target,
            ed.is_cohort,

            -- Monthly information
            mtd.reference_date,
            mtd.workforce_years,
            mtd.company_years,
            mtd.role_years,
            mtd.last_education_years
        FROM monthly_tenure_data mtd
        LEFT JOIN experience_data ed
            ON mtd.company_datasource_id = ed.company_datasource_id
            AND mtd.employee_id = ed.coresignal_employee_id
            AND mtd.experience_id = ed.experience_id
        LEFT JOIN classification cls
            ON mtd.company_datasource_id = cls.company_datasource_id
            AND mtd.experience_id = cls.experience_id
        LEFT JOIN dimension_config dc
            ON cls.job_function_group = dc.job_function_group
            AND cls.job_function = dc.job_function
        WHERE mtd.reference_date >= ed.date_from
            AND (
                mtd.reference_date <= ed.date_to
                OR ed.date_to IS NULL
            )
        """.format(
            summary_table_id=summary_table_id,
            table_coresignal_company_datasource=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
            table_coresignal_employees=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
            table_coresignal_employees_experiences=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES,
            table_hra_experience_function=config.GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION,
            table_hra_casestudy_dimension_config=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
            table_hra_employees_tenure_monthly=config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE_MONTHLY,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, summary_table_id
            )
        )
        raise error


def create_hra_summary_table_turnover(bqclient, case_study_id):
    table_id = None
    try:
        table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}"
        )
        query = f"""
            CREATE OR REPLACE TABLE `{table_id}`
            (
                case_study_id INT64,
                case_study_name STRING,
                company_id INT64,
                company_name STRING,
                time_key DATE,
                category_type STRING,
                category_value STRING,
                turnover_rate FLOAT64,
                num_employees_start INT64,
                num_employees_end INT64,
                num_starters INT64,
                num_leavers INT64
            );
        """
        query_job = bqclient.query(query)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] create_summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, table_id
            )
        )
        raise error


def insert_hra_summary_table_turnover(bqclient, case_study_id, case_study_name, run_id):
    logger.info(f"[MOVING_DATA] insert hra_summary_table_turnover_{case_study_id}")
    try:
        summary_table_id = (
            f"{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}"
        )
        query = ""
        query += build_query_company(case_study_id=case_study_id, case_study_name=case_study_name)
        query += build_query_company_job_function(case_study_id=case_study_id, case_study_name=case_study_name, run_id=run_id)
        query += build_query_company_tenure_bucket(case_study_id=case_study_id, case_study_name=case_study_name)
        query += build_query_company_country(case_study_id=case_study_id, case_study_name=case_study_name)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "case_study_id", "INTEGER", case_study_id
                ),
            ]
        )
        query_job = bqclient.query(query, job_config=job_config)
        query_job.result()
        return True

    except Exception as error:
        logger.exception(
            "[MOVING_DATA] insert summary_table: case_study_id={},error={},table={}".format(
                case_study_id, error.__class__.__name__, summary_table_id
            )
        )
        raise error
