import sys

sys.path.append("../")

from google.cloud import bigquery

import config


def clone_voc(from_case_study_id, to_case_study_id, logger):
    try:
        sql = f"""
        INSERT INTO `{config.GCP_BQ_TABLE_VOC}`
        SELECT 
            id, 
            created_at,	
            review_id, 
            source_name, 
            company_name,
            nlp_pack, 
            nlp_type, 
            user_name, 
            language, 
            review, 
            trans_review, 
            trans_status, 
            code, 
            dimension, 
            label, 
            terms, 
            relevance, 
            rel_relevance, 
            polarity, 
            rating, 
            batch_id,
            batch_name, 
            file_name, 
            review_date,
            company_id, 
            source_id, 
            step_id,
            request_id, 
            {to_case_study_id} as case_study_id, 
            parent_review_id, 
            technical_type 
        FROM `{config.GCP_BQ_TABLE_VOC}` 
        WHERE case_study_id = @case_study_id;
        """
        logger.info(sql)

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", from_case_study_id
            )
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] clone_voc: from_case_study_id={from_case_study_id}, to_case_study_id={to_case_study_id}"
        )
        return False


def clone_voe(from_case_study_id, to_case_study_id, logger):
    try:
        sql = f"""
        INSERT INTO `{config.GCP_BQ_TABLE_VOE}`
        SELECT 
            id, 
            created_at,
            review_id, 
            source_name, 
            company_name,
            nlp_pack, 
            nlp_type, 
            user_name, 
            language, 
            review,
            trans_review, 
            trans_status, 
            code, 
            dimension, 
            label, 
            terms, 
            relevance, 
            rel_relevance, 
            polarity, 
            rating, 
            batch_id,
            batch_name, 
            file_name, 
            review_date,
            company_id, 
            source_id, 
            step_id,
            request_id,
            {to_case_study_id} as case_study_id, 
            parent_review_id, 
            technical_type 
        FROM `{config.GCP_BQ_TABLE_VOE}` 
        WHERE case_study_id = @case_study_id;
        """
        logger.info(sql)

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", from_case_study_id
            )
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        return True
    except Exception as error:
        logger.error(
            f"[MOVING_DATA] clone_voe: from_case_study_id={from_case_study_id}, to_case_study_id={to_case_study_id}, from_batch_id={from_batch_id}, to_batch_id={to_batch_id}, error={error}"
        )
        return False


def clone_voe_company(from_case_study_id, to_case_study_id, logger):
    try:
        sql = f"""
        INSERT INTO `{config.GCP_BQ_TABLE_VOE_COMPANY}`
        
        SELECT 
            created_at, 
            {to_case_study_id} as case_study_id , 
            company_name, 
            company_id, 
            min_fte, 
            max_fte, 
            batch_id,
            source_name,
            source_id    
        FROM `{config.GCP_BQ_TABLE_VOE_COMPANY}` 
        WHERE case_study_id = @case_study_id;
        """
        logger.info(sql)

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", from_case_study_id
            )
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        return True
    except Exception as error:
        logger.error(
            f"[MOVING_DATA] clone_voe_company: from_case_study_id={from_case_study_id}, to_case_study_id={to_case_study_id}, from_batch_id={from_batch_id}, to_batch_id={to_batch_id}, error={error}"
        )
        return False


def clone_voe_job(from_case_study_id, to_case_study_id, logger):
    try:
        sql = f"""
        INSERT INTO `{config.GCP_BQ_TABLE_VOE_JOB}`
        SELECT 
            created_at, 
            {to_case_study_id} as case_study_id, 
            source_name, 
            source_id,
            company_name, 
            company_id,
            job_id,
            job_name, 
            job_function, 
            job_type, 
            posted_date, 
            job_country, 
            role_seniority,
            batch_id, 
            batch_name, 
            file_name, 
            request_id, 
            step_id   
        FROM `{config.GCP_BQ_TABLE_VOE_JOB}` 
        WHERE case_study_id = @case_study_id;
        """
        logger.info(sql)

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", from_case_study_id
            )
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        return True
    except Exception as error:
        logger.error(
            f"[MOVING_DATA] clone_voe_job: from_case_study_id={from_case_study_id}, to_case_study_id={to_case_study_id}, from_batch_id={from_batch_id}, to_batch_id={to_batch_id}, error={error}"
        )
        return False


def insert_to_voc(row_batch, logger):
    """
    Insert given list of rows into table staging.voc
    """
    required_keys = (
        "created_at",
        "review_id",
        "source_name",
        "company_name",
        "nlp_pack",
        "nlp_type",
        "user_name",
        "language",
        "review",
        "trans_review",
        "trans_status",
        "code",
        "dimension",
        "label",
        "terms",
        "relevance",
        "rel_relevance",
        "polarity",
        "rating",
        "batch_id",
        "batch_name",
        "file_name",
        "review_date",
        "company_id",
        "source_id",
        "step_id",
        "request_id",
        "case_study_id",
        "parent_review_id",
        "technical_type",
    )
    try:
        # construct client
        client = bigquery.Client()
        errors = client.insert_rows_json(config.GCP_BQ_TABLE_VOC, row_batch)
        if errors:
            logger.error(
                f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}: "
                f"{errors}"
            )
    except Exception as error:
        logger.exception(
            f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}"
        )
        raise error


def insert_to_voe(row_batch, logger):
    """
    Insert given list of rows into table staging.voc
    """
    required_keys = (
        "created_at",
        "review_id",
        "source_name",
        "company_name",
        "nlp_pack",
        "nlp_type",
        "user_name",
        "language",
        "review",
        "trans_review",
        "trans_status",
        "code",
        "dimension",
        "label",
        "terms",
        "relevance",
        "rel_relevance",
        "polarity",
        "rating",
        "batch_id",
        "batch_name",
        "file_name",
        "review_date",
        "company_id",
        "source_id",
        "step_id",
        "request_id",
        "case_study_id",
        "parent_review_id",
        "technical_type",
    )
    try:
        # construct client
        client = bigquery.Client()
        errors = client.insert_rows_json(config.GCP_BQ_TABLE_VOC, row_batch)
        if errors:
            logger.error(
                f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}: "
                f"{errors}"
            )
    except Exception as error:
        logger.exception(
            f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}"
        )
        raise error


def insert_to_voe_job(row_batch, logger):
    """
    Insert given list of rows into table staging.voc
    """
    required_keys = (
        "created_at",
        "case_study_id",
        "source_name",
        "source_id",
        "company_name",
        "company_id",
        "job_id",
        "job_name",
        "job_function",
        "job_type",
        "posted_date",
        "job_country",
        "role_seniority",
        "batch_id",
        "batch_name",
        "file_name",
        "request_id",
        "step_id",
    )

    try:
        # construct client
        client = bigquery.Client()
        errors = client.insert_rows_json(config.GCP_BQ_TABLE_VOE_JOB, row_batch)
        if errors:
            logger.error(
                f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}: "
                f"{errors}"
            )
    except Exception as error:
        logger.exception(
            f"[insert_to_voc] error happened while inserting to {config.GCP_BQ_TABLE_VOC}"
        )
        raise error
