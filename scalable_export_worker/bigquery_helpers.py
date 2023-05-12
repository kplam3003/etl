import sys

sys.path.append("../")

from google.cloud import bigquery
import config
import datetime as dt
import pandas as pd
import re
import ast
import json


def insert_casestudy_batchid(
    logger,
    bqclient,
    case_study_id,
    request_id,
    batch_id,
    batch_name,
    company_id,
    source_id,
    created_at,
    run_id,
    table_name,
):
    logger.info(f"Before insert bigquery table_name={table_name}")
    insert_rows_json(
        logger,
        bqclient,
        table_name,
        [
            {
                "created_at": created_at,
                "case_study_id": case_study_id,
                "request_id": request_id,
                "batch_id": batch_id,
                "batch_name": batch_name,
                "company_id": company_id,
                "source_id": source_id,
                "run_id": run_id,
            }
        ],
    )
    logger.info(f"Insert successfull bigquery table_name={table_name}")


def insert_rows_json(logger, bqclient, table_name, items):
    logger.info(
        f"Before insert bigquery table_name={table_name}, total={len(items)}, first_item={items[0]}"
    )
    # bqclient = bigquery.Client()
    errors = bqclient.insert_rows_json(table_name, items)
    if errors != []:
        logger.error("Encountered errors while inserting rows: {}".format(errors))
        raise Exception("Bigquery insert fail")
    logger.info(
        f"Insert successfull bigquery table_name={table_name}, total={len(items)}, first_item={items[0]}"
    )


def insert_polarity_trans(
    logger,
    bqclient,
    case_study_id,
    case_study_name,
    nlp_polarity,
    modified_polarity,
    run_id,
    table_polarity_trans,
):
    logger.info(
        f"Before insert bigquery table_name={table_polarity_trans}, with (case_study_id={case_study_id}, case_study_name={case_study_name}, nlp_polarity={nlp_polarity}, modified_polarity={modified_polarity})"
    )
    insert_rows_json(
        logger,
        bqclient,
        table_polarity_trans,
        [
            {
                "created_at": dt.datetime.now().isoformat(),
                "case_study_id": case_study_id,
                "case_study_name": case_study_name,
                "nlp_polarity": nlp_polarity,
                "modified_polarity": modified_polarity,
                "run_id": run_id,
            }
        ],
    )
    logger.info(
        f"Insert successfull bigquery table_name={table_polarity_trans}, with (case_study_id={case_study_id}, case_study_name={case_study_name}, nlp_polarity={nlp_polarity}, modified_polarity={modified_polarity})"
    )


def insert_casestudy_company_source(
    logger,
    bqclient,
    case_study_id,
    case_study_name,
    companies,
    dimension_config_id,
    run_id,
    request_payload,
    table_casestudy_company_source,
):
    logger.info(f"Before insert bigquery table_name={table_casestudy_company_source}")
    today = dt.datetime.now()
    items = []

    created_at = today.isoformat()

    # check for duplicated ids and
    processed_companies = set()
    for company in companies:
        if (company["company_id"], company["source_id"]) in processed_companies:
            continue
        else:
            processed_companies.add((company["company_id"], company["source_id"]))

        item = {}
        item["created_at"] = created_at
        item["case_study_id"] = case_study_id
        item["case_study_name"] = case_study_name
        item["company_id"] = company["company_id"]
        item["company_name"] = company["company_name"]
        item["source_id"] = company["source_id"]
        item["source_name"] = company["source_name"]
        item["nlp_pack"] = company["nlp_pack"]
        item["nlp_type"] = company["nlp_type"]
        item["is_target"] = company["is_target"]
        item["dimension_config_id"] = dimension_config_id
        item["run_id"] = run_id
        item["abs_relevance_inf"] = float(request_payload["absolute_relevance"])
        item["rel_relevance_inf"] = float(request_payload["relative_relevance"])
        items.append(item)

    insert_rows_json(logger, bqclient, table_casestudy_company_source, items)
    logger.info(
        f"Insert successfull bigquery table_name={table_casestudy_company_source}"
    )


# WILL BE UPDATE FROM WEB SYNC PAYLOAD
def insert_casestudy_dimension_config(
    logger,
    bqclient,
    case_study_id,
    case_study_name,
    dimension_config_id,
    run_id,
    payload,
    table_casestudy_dimension_config_name,
):
    logger.info(
        f"Before insert bigquery table_name={table_casestudy_dimension_config_name}"
    )
    # begin select from dimension_default:
    try:
        items = []
        created_at = dt.datetime.now().isoformat()

        nlp_pack = payload["dimension_config"]["nlp_pack"]
        nlp_type = payload["dimension_config"]["nlp_type"]
        name = payload["dimension_config"]["name"]
        dimensions = payload["dimension_config"]["dimensions"]

        for dimension in dimensions:
            for label in dimension["labels"]:
                item = {}
                item["created_at"] = created_at
                item["case_study_id"] = case_study_id
                item["case_study_name"] = case_study_name
                item["nlp_pack"] = nlp_pack
                item["nlp_type"] = nlp_type
                item["dimension_config_id"] = dimension_config_id
                item["dimension_config_name"] = name
                item["nlp_dimension"] = dimension["nlp"]
                item["modified_dimension"] = dimension["modified"]
                item["nlp_label"] = label["nlp"]
                item["modified_label"] = label["modified"]
                item["is_used"] = bool(dimension["is_use_for_analysis"])
                item["dimension_type"] = dimension["type"]
                item["run_id"] = run_id
                item["keywords"] = "\t".join(label.get("keywords", []))
                item["is_user_defined"] = label.get("is_user_defined", False)
                items.append(item)

        insert_rows_json(logger, bqclient, table_casestudy_dimension_config_name, items)
        logger.info(
            f"\nCase_study_id {case_study_id} is inserted successfully into {table_casestudy_dimension_config_name} Biquery table !"
        )
    except (Exception) as error:
        logger.error(
            "\n There is error query table dimension_default in Bigquery: {error}"
        )
        raise error
    # end select from dimension_default:
    logger.info(
        f"Insert successfull bigquery table_name={table_casestudy_dimension_config_name}"
    )


def _check_existing_parent_review_mappings(
    client, case_study_id, parent_review_mapping_table_id, logger
):
    """
    Check if parent review mapping for a case study has been inserted
    """
    try:
        logger.info(
            f"Checking if parent_review_mapping for this case_study_id {case_study_id}..."
        )
        statement = f"""
            SELECT COUNT(*) AS row_count
            FROM `{parent_review_mapping_table_id}`
            WHERE case_study_id = {case_study_id}
        """
        query_job = client.query(statement)
        query_results = query_job.result()
        # check query result
        for row in query_results:
            if row.row_count != 0:
                # has reviews => already parsed once
                return True
            else:
                # no review => not parsed yet
                return False

    except Exception as error:
        logger.exception(
            f"[CHECK] Cannot check parent_review_mapping status for case_study_id={case_study_id}, error={error}"
        )
        raise


def _check_existing_review_country_mapping(
    client, case_study_id, review_country_mapping_table_id, logger
):
    """
    Check if parent review mapping for a case study has been inserted
    """
    try:
        logger.info(
            f"Checking if review_country_mapping for this case_study_id {case_study_id}..."
        )
        statement = f"""
            SELECT COUNT(*) AS row_count
            FROM `{review_country_mapping_table_id}`
            WHERE case_study_id = {case_study_id}
        """
        query_job = client.query(statement)
        query_results = query_job.result()
        # check query result
        for row in query_results:
            if row.row_count != 0:
                # has reviews => already parsed once
                return True
            else:
                # no review => not parsed yet
                return False

    except Exception as error:
        logger.exception(
            f"[CHECK] Cannot check review_country_mapping status for case_study_id={case_study_id}, error={error}"
        )
        raise


def insert_table_casestudy_custom_dimension_statistics(
    logger,
    bqclient,
    case_study_id,
    case_study_name,
    dimension_config_id,
    dimension_config_name,
    run_id,
    table_staging_custom_dimension_review,
    table_casestudy_dimension_config,
    table_casestudy_custom_dimension_statistics,
    table_batch_status,
    table_casestudy_batchid,
):
    """
    Calculate term count and review count for user-defined dimensions
    Uses table casestudy_batchid and batch_status so this needs to come after inserting
    those.
    """
    logger.info(
        f"Before insert bigquery table_name={table_casestudy_custom_dimension_statistics}"
    )
    sql = """
    INSERT INTO {table_custom_dimension_statistics}
    WITH BATCH_LIST AS (
        SELECT
            batch_id
        FROM
            `{table_batch_status}`
        WHERE
            batch_id in (
                SELECT
                    batch_id
                FROM
                    `{table_casestudy_batchid}`
                WHERE
                    case_study_id = {case_study_id}
            )
            AND status = 'Active'
    ),
    custom_dimension_words AS (
        SELECT
            nlp_dimension AS dimension,
            nlp_label AS label,
            LOWER(word) AS lowered_word,
            word
        FROM 
            `{table_casestudy_dimension_config}`,
            UNNEST(SPLIT(keywords, '\t')) as word
        WHERE 
            case_study_id = {case_study_id}
            AND run_id = '{run_id}'
            AND is_user_defined = True
    ),
    custom_dimension_words_data AS (
        SELECT
            review_id,
            dimension,
            label,
            word
        FROM
            `{table_staging_custom_dimension_review}` a,
            UNNEST(SPLIT(LOWER(terms), '\t')) as word
        WHERE
            a.batch_id IN (
                SELECT 
                    batch_id 
                FROM 
                    BATCH_LIST
            )
            AND a.case_study_id = {case_study_id}
            AND a.run_id = '{run_id}'
    ),
    case_study_dimension_words_stats AS (
        SELECT
            CURRENT_TIMESTAMP() AS created_at,
            {case_study_id} AS case_study_id,
            '{case_study_name}' AS case_study_name,
            {dimension_config_id} AS dimension_config_id,
            '{dimension_config_name}' AS dimension_config_name,
            dimension,
            label,
            word,
            COUNT(*) AS word_count,
            COUNT(DISTINCT review_id) as review_count,
            '{run_id}' AS run_id,
        FROM 
            custom_dimension_words_data
        GROUP BY
            dimension,
            label,
            word
    )
    SELECT
        a.created_at,
        a.case_study_id,
        a.case_study_name,
        a.dimension_config_id,
        a.dimension_config_name,
        a.dimension,
        a.label,
        b.word,
        CASE
            WHEN a.word_count IS NULL THEN 0
            ELSE a.word_count
        END AS word_count,
        CASE
            WHEN a.review_count IS NULL THEN 0
            ELSE a.review_count
        END AS review_count,
        a.run_id
    FROM case_study_dimension_words_stats a
        LEFT JOIN custom_dimension_words b
        ON a.word = b.lowered_word
        AND a.dimension = b.dimension
        AND a.label = b.label
    ;
    """.format(
        table_custom_dimension_statistics=table_casestudy_custom_dimension_statistics,
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        run_id=run_id,
        dimension_config_id=dimension_config_id,
        dimension_config_name=dimension_config_name,
        table_staging_custom_dimension_review=table_staging_custom_dimension_review,
        table_casestudy_dimension_config=table_casestudy_dimension_config,
        table_batch_status=table_batch_status,
        table_casestudy_batchid=table_casestudy_batchid,
    )
    try:
        query_job = bqclient.query(sql)
        query_job.result()
        logger.info(
            f"Insert successfully into {table_casestudy_custom_dimension_statistics} Biquery table !"
        )
        return True

    except Exception as error:
        logger.exception(
            f"Failed to insert to {table_casestudy_custom_dimension_statistics} Biquery table !"
        )
        return False


def insert_case_study_run_id(logger, bqclient, case_study_id, run_id, table_name):
    logger.info(
        f"Before insert bigquery table_name={config.GCP_BQ_TABLE_CASESTUDY_RUN_ID}"
    )
    item = {
        "created_at": dt.datetime.now().isoformat(),
        "case_study_id": case_study_id,
        "run_id": run_id,
    }
    insert_rows_json(logger, bqclient, table_name, [item])
    logger.info(f"Insert successfull bigquery table_name={table_name}")


def insert_company_aliases(
    bqclient,
    case_study_id,
    companies_data_payload,
    run_id,
    table_company_aliases_list,
    logger,
):
    """
    Construct company with aliases and insert into table

    Params
    ------
        company_data_sources_payload: List[Dict]
            List of company_details

    """
    try:
        _existed_ids = set()
        items = []
        for company_details in companies_data_payload:
            _id = company_details["company_id"]
            # due to one company may have multiple sources, company details are duplicated.
            if _id in _existed_ids:
                continue
            else:
                _existed_ids.add(_id)

            # original alias for safekeeping
            _original_aliases_list = company_details.get("company_aliases", [])
            _original_aliases = "\t".join(
                _original_aliases_list
            )  # save as string, tab separated
            # aliases with escaped regex metacharacter for BQ queries
            _escaped_aliases_list = [re.escape(s) for s in _original_aliases_list]
            _escaped_aliases = "\t".join(_escaped_aliases_list)

            _name = company_details["company_name"]

            items.append(
                {
                    "created_at": dt.datetime.now().isoformat(),
                    "case_study_id": case_study_id,
                    "company_id": _id,
                    "company_name": _name,
                    "aliases": _escaped_aliases,  # escaped aliased for queries
                    "original_aliases": _original_aliases,  # original for safekeeping/reference
                    "run_id": run_id,
                }
            )

        logger.info(
            f"Before insert bigquery table_name={table_company_aliases_list}, num_rows={len(items)}"
        )
        insert_rows_json(logger, bqclient, table_company_aliases_list, items)
        logger.info(
            f"Insert successfully to bigquery table_name={table_company_aliases_list}"
        )
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] insert_company_aliases: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_dimension_keyword_list(
    bqclient,
    case_study_id,
    keywords_include,
    keywords_exclude,
    run_id,
    table_dimension_keyword_list,
    logger
):
    try:
        keywords_include = "\t".join(keywords_include.split(","))
        keywords_exclude = "\t".join(keywords_exclude.split(","))
        logger.info(f"Before insert bigquery table_name={table_dimension_keyword_list}")
        item = {
            "created_at": dt.datetime.now().isoformat(),
            "case_study_id": case_study_id,
            "in_list": keywords_include,
            "ex_list": keywords_exclude,
            "run_id": run_id,
        }
        insert_rows_json(logger, bqclient, table_dimension_keyword_list, [item])
        logger.info(
            f"Insert successfull bigquery table_name={table_dimension_keyword_list}"
        )
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] insert_dimension_keyword_list: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def create_tmp_table_to_bigquery(bqclient, src_table, dest_table_id, case_study_id, logger):
    try:
        bqclient.delete_table(dest_table_id, not_found_ok=True)

        job_config = bigquery.QueryJobConfig(destination=dest_table_id)

        sql = """
        SELECT * FROM `{}.{}.{}` WHERE case_study_id = {};
        """.format(
            config.GCP_PROJECT_ID,
            config.GCP_BQ_DATASET_ID_DATAMART,
            src_table,
            case_study_id,
        )

        # Start the query, passing in the extra configuration.
        query_job = bqclient.query(sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        return True
    except Exception as error:
        logger.exception(f"[BIGQUERY] create table fail: {error}")
        return False


def export_to_gcs(bqclient, table_id, file_path_export, logger):
    try:
        destination_uri = "gs://{}/{}".format(
            config.GCP_STORAGE_BUCKET, f"export/{file_path_export}"
        )
        dataset_ref = bigquery.DatasetReference(
            config.GCP_PROJECT_ID, config.GCP_BQ_DATASET_ID_EXPORT
        )
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.ExtractJobConfig(field_delimiter="\t")

        extract_job = bqclient.extract_table(
            table_ref, destination_uri, job_config=job_config
        )  # API request
        extract_job.result()  # Waits for job to complete.

        logger.info(
            "   Exported {}:{}.{} to {}".format(
                config.GCP_PROJECT_ID,
                config.GCP_BQ_DATASET_ID_EXPORT,
                table_id,
                destination_uri,
            )
        )

        return True
    except Exception as error:
        logger.exception(f"[GCS] export table fail: {error}")
        return False


def export_chart(bqclient, chart_code, payload, metadata, logger):
    try:
        logger.info(f"[EXPORT_BEGIN] chart:{chart_code}")
        src_table = metadata[chart_code]["datamart_table"]
        prefix_filename = metadata[chart_code]["prefix_filename"]
        chart_code = chart_code.lower()
        tmp_table = f"{prefix_filename}_csid_{payload['case_study_id']}"
        tmp_table_id = (
            f"{config.GCP_PROJECT_ID}.{config.GCP_BQ_DATASET_ID_EXPORT}.{tmp_table}"
        )

        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, create_tmp_table_begin: {tmp_table_id}"
        )
        if not create_tmp_table_to_bigquery(
            bqclient, src_table, tmp_table_id, payload["case_study_id"], logger
        ):
            logger.info(
                f"  [EXPORT_PROGRESS] chart: {chart_code}, create_tmp_table_fail: {tmp_table_id}"
            )
            return
        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, create_tmp_table_success: {tmp_table_id}"
        )

        file_path_export = f"{chart_code}/{tmp_table}/{tmp_table}-*.csv"
        # file_path_export = f"{chart_code}/{tmp_table}.csv"
        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, export_to_gcs_file_path_begin: {file_path_export}"
        )
        if not export_to_gcs(bqclient, tmp_table, file_path_export, logger):
            logger.info(
                f"  [EXPORT_PROGRESS] chart: {chart_code}, export_to_gcs_file_path_fail: {file_path_export}"
            )
            return
        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, export_to_gcs_file_path_success: {file_path_export}"
        )

        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, delete_tmp_table_start: {tmp_table_id}"
        )
        try:
            bqclient.delete_table(tmp_table_id, not_found_ok=True)
        except Exception as error:
            logger.exception(
                f"  [EXPORT_PROGRESS] chart: {chart_code}, delete_tmp_table_fail: {tmp_table_id}"
            )
        logger.info(
            f"  [EXPORT_PROGRESS] chart: {chart_code}, delete_tmp_table_success: {tmp_table_id}"
        )

        logger.info(f"[EXPORT_SUCCESS] chart: {chart_code}")
    except Exception as error:
        logger.exception(f"[EXPORT_ERROR] chart: {chart_code}, error: {error}")

