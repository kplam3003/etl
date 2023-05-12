import sys

sys.path.append("../")

import uuid
from google.cloud import bigquery

import os
import datetime
import datetime as dt
import time

import config
from core import pubsub_util, logger, common_util, etl_const
from core.operation import GracefulKiller, auto_killer
import bigquery_helpers
import hra_coresignal
import utils

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

metadata = {
    # csv exports for VoC charts
    "VOC_1_1": {
        "datamart_table": "VOC_1_1_cusrevtimecompany_v",
        "prefix_filename": "customer_reviews_count_over_time",
    },
    "VOC_1_2": {
        "datamart_table": "VOC_1_2_cusrevstat_v",
        "prefix_filename": "reviews_statistics",
    },
    "VOC_1_3_1": {
        "datamart_table": "VOC_1_3_1_cusrevcompany_p_v",
        "prefix_filename": "customer_reviews_collected_by_players",
    },
    "VOC_1_3_2": {
        "datamart_table": "VOC_1_3_2_cusrevcompany_b_v",
        "prefix_filename": "reviews_count_by_players_and_data_sources",
    },
    "VOC_1_4": {
        "datamart_table": "VOC_1_4_cusrevtimelangue_v",
        "prefix_filename": "collected_customer_reviews_count_by_language",
    },
    "VOC_1_4_1": {
        "datamart_table": "VOC_1_4_1_cusrevtimecountry_v",
        "prefix_filename": "collected_customer_reviews_count_by_country",
    },
    "VOC_1_5": {
        "datamart_table": "VOC_1_5_cusrevlangue_p_v",
        "prefix_filename": "collected_customer_reviews_count_by_language",
    },
    "VOC_1_5_1": {
        "datamart_table": "VOC_1_5_1_cusrevcountry_p_v",
        "prefix_filename": "collected_customer_reviews_count_by_country",
    },
    "VOC_1_6": {
        "datamart_table": "VOC_1_6_cusrevtimesource_v",
        "prefix_filename": "reviews_count_by_data_source",
    },
    "VOC_1_7": {
        "datamart_table": "VOC_1_7_cusrevprocessed_v",
        "prefix_filename": "reviews_collected_and_processed_successfully",
    },
    "VOC_2": {
        "datamart_table": "VOC_2_polaritydistr_v",
        "prefix_filename": "reviews_distribution_per_polarity",
    },
    "VOC_3": {
        "datamart_table": "VOC_3_ratingtime",
        "prefix_filename": "customer_review_ratings",
    },
    "VOC_4_1": {
        "datamart_table": "VOC_4_1_sstimecompany",
        "prefix_filename": "sentiment_scores_by_players_over_time",
    },
    "VOC_4_2": {
        "datamart_table": "VOC_4_2_cusrevsstime",
        "prefix_filename": "sentiment_scores_and_customer_reviews_collected_over_time",
    },
    "VOC_5": {
        "datamart_table": "VOC_5_heatmapdim",
        "prefix_filename": "customer_reviews_count_per_dimension",
    },
    "VOC_6_1": {
        "datamart_table": "VOC_6_1_sstimedimcompany",
        "prefix_filename": "dimension_sentiment_score_per_company",
    },
    "VOC_6_2": {
        "datamart_table": "VOC_6_2_dimentioned",
        "prefix_filename": "dimensions_mentioned_in_reviews",
    },
    "VOC_6_3": {
        "datamart_table": "VOC_6_3_dimpolarity",
        "prefix_filename": "dimensions_mentioned_positive_and_negative_polarity",
    },
    "VOC_6_4": {
        "datamart_table": "VOC_6_4_dimss",
        "prefix_filename": "sentiment_scores_ranking_per_dimension",
    },
    "VOC_6_5": {
        "datamart_table": "VOC_6_5_competitor",
        "prefix_filename": "competitors_overlap_analysis",
    },
    "VOC_6_6": {
        "datamart_table": "VOC_6_6_sstimesourcecompany",
        "prefix_filename": "sentiment_score_by_data_source_per_company",
    },
    "VOC_6_7": {
        "datamart_table": "VOC_6_7_commonterms",
        "prefix_filename": "most_common_terms",
    },
    "VOC_6_7_1": {
        "datamart_table": "VOC_6_7_1_commonkeywords",
        "prefix_filename": "most_common_keywords",
    },
    "nlp_output": {"datamart_table": "nlp_output", "prefix_filename": "nlp_output"},
    "review_raw_data": {
        "datamart_table": "review_raw_data",
        "prefix_filename": "customer_review_raw_data",
    },
    "VOC_12": {
        "datamart_table": "VOC_12_sscausalmining",
        "prefix_filename": "sentiment_causal_mining",
    },
    "VOC_12_1": {
        "datamart_table": "VOC_12_1_sscausaldim",
        "prefix_filename": "sentiment_causal_mining_sentiment_by_dimensions",
    },
    "VOC_12_2": {
        "datamart_table": "VOC_12_2_sscausalpolarity",
        "prefix_filename": "sentiment_causal_mining_review_polarity",
    },
    "VOC_11": {
        "datamart_table": "VOC_11_profilestats",
        "prefix_filename": "profile_stats",
    },
    "VOC_13": {
        "datamart_table": "VOC_13_ratingtimecompany",
        "prefix_filename": "self_reported_customer_scores_by_players_over_time",
    },
    "VOC_14": {"datamart_table": "VOC_14_nvsrating", "prefix_filename": "n_vs_rating"},
    "VOC_15": {
        "datamart_table": "VOC_15_nvsratingtime",
        "prefix_filename": "n_vs_rating_over_time",
    },
    "VOC_16": {
        "datamart_table": "VOC_16_kpcsentcompany",
        "prefix_filename": "kpc_prevalence_per_sentiment_company",
    },
    # csv exports for VoE charts
    "VOE_7_1": {
        "datamart_table": "VOE_7_1_jobfunctionquant",
        "prefix_filename": "total_job_listings_by_function_-_identifying_hiring_concentration_by_company",
    },
    "VOE_7_2": {
        "datamart_table": "VOE_7_2_jobfunctionpercent",
        "prefix_filename": "_of_jobs_listed_by_function_v_company_fte_count",
    },
    "VOE_7_3": {
        "datamart_table": "VOE_7_3_jobfunctionvFTE",
        "prefix_filename": "hiring_percentage_by_function",
    },
    "VOE_7_4": {
        "datamart_table": "VOE_7_4_jobbycountry_t",
        "prefix_filename": "hiring_by_country",
    },
    "VOE_7_6": {
        "datamart_table": "VOE_7_6_jobfunctionbyday",
        "prefix_filename": "average_number_of_days_a_functional_job_has_been_listed",
    },
    "VOE_8_1": {
        "datamart_table": "VOE_8_1_roleseniorquant",
        "prefix_filename": "role_seniority_levels",
    },
    "VOE_8_4": {
        "datamart_table": "VOE_8_4_roleseniorbyjobtype",
        "prefix_filename": "role_seniority_by_job_typeindustry",
    },
    "voe_nlp_output": {
        "datamart_table": "voe_nlp_output",
        "prefix_filename": "voe_nlp_output",
    },
    "voe_review_raw_data": {
        "datamart_table": "voe_review_raw_data",
        "prefix_filename": "employee_review_raw_data",
    },
    "voe_job_postings": {
        "datamart_table": "voe_job_postings",
        "prefix_filename": "job_postings_data",
    },
    "VOE_1_2": {
        "datamart_table": "VOE_1_2_emprevstat_v",
        "prefix_filename": "employee_review_statistics",
    },
    "VOE_1_3_2": {
        "datamart_table": "VOE_1_3_2_emprevcompany_b_v",
        "prefix_filename": "reviews_count_by_players_and_data_sources",
    },
    "VOE_1_4": {
        "datamart_table": "VOE_1_4_emprevtimelangue_v",
        "prefix_filename": "collected_employee_reviews_count_by_language",
    },
    "VOE_1_5": {
        "datamart_table": "VOE_1_5_emprevlangue_p_v",
        "prefix_filename": "collected_employee_reviews_count_by_language",
    },
    "VOE_1_6": {
        "datamart_table": "VOE_1_6_emprevtimesource_v",
        "prefix_filename": "reviews_count_by_data_source",
    },
    "VOE_1_7": {
        "datamart_table": "VOE_1_7_emprevprocessed_v",
        "prefix_filename": "reviews_collected_and_processed_successfully",
    },
    "VOE_2": {
        "datamart_table": "VOE_2_polaritydistr_v",
        "prefix_filename": "reviews_distribution_per_polarity",
    },
    "VOE_3": {
        "datamart_table": "VOE_3_ratingtime",
        "prefix_filename": "employee_review_ratings",
    },
    "VOE_4_2": {
        "datamart_table": "VOE_4_2_emprevsstime",
        "prefix_filename": "sentiment_scores_and_employee_reviews_collected_over_time",
    },
    "VOE_5": {
        "datamart_table": "VOE_5_heatmapdim",
        "prefix_filename": "employee_reviews_count_per_dimension",
    },
    "VOE_6_1": {
        "datamart_table": "VOE_6_1_sstimedimcompany",
        "prefix_filename": "dimension_sentiment_score_per_company",
    },
    "VOE_6_2": {
        "datamart_table": "VOE_6_2_dimentioned",
        "prefix_filename": "dimensions_mentioned_in_employee_reviews",
    },
    "VOE_6_3": {
        "datamart_table": "VOE_6_3_dimpolarity",
        "prefix_filename": "dimensions_mentioned_positive_and_negative_polarity",
    },
    "VOE_6_5": {
        "datamart_table": "VOE_6_5_competitor",
        "prefix_filename": "competitors_overlap_analysis",
    },
    "VOE_6_6": {
        "datamart_table": "VOE_6_6_sstimesourcecompany",
        "prefix_filename": "sentiment_score_by_data_source_per_company",
    },
    "VOE_7_5": {
        "datamart_table": "VOE_7_5_jobbycountry_p",
        "prefix_filename": "job_by_country_piechart",
    },
    "VOE_9_1": {
        "datamart_table": "VOE_9_1_reviewsbycompany_p",
        "prefix_filename": "reviews_by_company",
    },
    "VOE_9_3": {
        "datamart_table": "VOE_9_3_ssaverage",
        "prefix_filename": "average_sentiment_score",
    },
    "VOE_9_4": {
        "datamart_table": "VOE_9_4_reviewsbycompany",
        "prefix_filename": "review_count_by_company",
    },
    "VOE_9_5": {
        "datamart_table": "VOE_9_5_ssbytimecompany",
        "prefix_filename": "sentiment_by_company_over_time",
    },
    "VOE_11": {
        "datamart_table": "VOE_11_termcount",
        "prefix_filename": "voe_most_common_terms",
    },
    "VOE_11_1": {
        "datamart_table": "VOE_11_1_keywordcount",
        "prefix_filename": "voe_most_common_keywords",
    },
    "VOE_10_1": {
        "datamart_table": "VOE_10_1_dimss",
        "prefix_filename": "voe_signal_sentiment_scores",
    },
    "VOE_12": {
        "datamart_table": "VOE_12_ratingtimecompany",
        "prefix_filename": "self_reported_employee_scores_by_players_over_time"
    },
    "VOE_12_1": {
        "datamart_table": "VOE_12_1_avgratingcompany",
        "prefix_filename": "average_employee_self_reported_score"
    },
    "VOE_12_2": {
        "datamart_table": "VOE_12_2_avgratingperiodcompany",
        "prefix_filename": "average_period_employee_self_reported_score"
    },
    "VOE_13": {
        "datamart_table": "VOC_13_profilestats",
        "prefix_filename": "profile_stats",
    },
    "VOE_14": {"datamart_table": "VOE_14_nvsrating", "prefix_filename": "n_vs_rating"},
    "VOE_15": {
        "datamart_table": "VOE_15_nvsratingtime",
        "prefix_filename": "n_vs_rating_over_time",
    },
    "VOE_16": {
        "datamart_table": "VOE_16_kpcsentcompany",
        "prefix_filename": "kpc_prevalence_per_sentiment_company",
    },
}

@auto_killer
def handle_task_voc_voe(payload, set_terminator=None):
    try:
        run_id = payload["postprocess"]["meta_data"]["run_id"]
    except:
        logger.warning(f"run_id is not readily available in payload")
        run_id = str(uuid.uuid1())

    try:
        bqclient = bigquery.Client()
        # update initial progress
        utils.update_progress(
            payload=payload,
            worker_progress=0.0,
            project_id=config.GCP_PROJECT_ID,
            progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
            logger=logger
        )

        # Set terminator for before kill execution
        def _terminator():
            logger.info("Grateful terminating...")
            pubsub_util.publish(
                logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC, payload
            )
            logger.info(
                f"Republishing a message to {config.GCP_PUBSUB_TOPIC}: {payload}"
            )
            os._exit(1)

        set_terminator(_terminator)

        logger.info(f"Begin moving_voc_to_next_step")
        # voc -> datamart, dwh
        table_map = {}

        if payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoC".lower():
            table_map = {
                # data_type = review
                "table_casestudy_run_id": config.GCP_BQ_TABLE_CASESTUDY_RUN_ID,
                "table_casestudy_batchid": config.GCP_BQ_TABLE_CASESTUDY_BATCHID,
                "table_casestudy_dimension_config": config.GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG,
                "table_casestudy_company_source": config.GCP_BQ_TABLE_CASESTUDY_COMPANY_SOURCE,
                "table_polarity_trans": config.GCP_BQ_TABLE_POLARITY_TRANS,
                "table_dimension_keyword_list": config.GCP_BQ_TABLE_DIMENSION_KEYWORD_LIST,
                "table_batch_status": config.GCP_BQ_TABLE_BATCH_STATUS,
                "table_casestudy_batchid": config.GCP_BQ_TABLE_CASESTUDY_BATCHID,
                "table_summary_table_prefix": config.GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX,
                "table_nlp_output_case_study": config.GCP_BQ_TABLE_NLP_OUTPUT_CASE_STUDY,
                "table_language_trans": config.GCP_BQ_TABLE_LANGUAGE_TRANS,
                "staging_review": config.GCP_BQ_TABLE_VOC,
                "staging_review_stats": config.GCP_BQ_TABLE_VOC_REVIEW_STATS,
                "staging_custom_dimension_review": config.GCP_BQ_TABLE_VOC_CUSTOM_DIMENSION,
                "table_casestudy_custom_dimension_statistics": config.GCP_BQ_TABLE_VOC_CASESTUDY_CUSTOM_DIMENSION_STATISTICS,
                "table_company_aliases": config.GCP_BQ_TABLE_COMPANY_ALIASES_LIST,
                "table_parent_review_mapping": config.GCP_BQ_TABLE_PARENT_REVIEW_MAPPING,
                # if a VOC chart needs data from a materialized table for optimization:
                # 1. table key and id need to be declared (here)
                # 2. an insert function (Bigquery SQL) needs to be constructed (at the end)
                # 3. table must be added to export_all_chart function, VoC part (below)
                "table_6_7_commonterms_table": "VOC_6_7_commonterms_table",
                "table_6_7_1_commonkeywords_table": "VOC_6_7_1_commonkeywords_table",
                "table_6_5_competitor_table": "VOC_6_5_competitor_table",
                "table_5_heatmapdim_table": "VOC_5_heatmapdim_table",
                "table_3_ratingtime_table": "VOC_3_ratingtime_table",
            }

            moving_staging_to_next_step_for_review_and_job(
                payload, bqclient, table_map, run_id
            )

            # update progress
            utils.update_progress(
                payload=payload,
                worker_progress=0.4,
                project_id=config.GCP_PROJECT_ID,
                progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                logger=logger
            )

            moving_staging_to_next_step_for_review(payload, bqclient, table_map, run_id)

            # update progress
            utils.update_progress(
                payload=payload,
                worker_progress=0.8,
                project_id=config.GCP_PROJECT_ID,
                progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                logger=logger
            )

            # export all chart
            logger.info(f"Begin export_all_chart VoC")
            export_all_chart(bqclient, payload, run_id, table_map)

        elif (
            payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoE".lower()
        ):
            table_map = {
                # data_type = review, job
                "table_casestudy_run_id": config.GCP_BQ_TABLE_VOE_CASESTUDY_RUN_ID,
                "table_casestudy_batchid": config.GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID,
                "table_casestudy_dimension_config": config.GCP_BQ_TABLE_VOE_CASESTUDY_DIMENSION_CONFIG,
                "table_casestudy_company_source": config.GCP_BQ_TABLE_VOE_CASESTUDY_COMPANY_SOURCE,
                "table_polarity_trans": config.GCP_BQ_TABLE_VOE_POLARITY_TRANS,
                "table_dimension_keyword_list": config.GCP_BQ_TABLE_VOE_DIMENSION_KEYWORD_LIST,
                "table_batch_status": config.GCP_BQ_TABLE_VOE_BATCH_STATUS,
                "table_casestudy_batchid": config.GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID,
                "table_summary_table_prefix": config.GCP_BQ_TABLE_SUMMARY_TABLE_VOE_PREFIX,
                "table_nlp_output_case_study": config.GCP_BQ_TABLE_VOE_NLP_OUTPUT_CASE_STUDY,
                "table_language_trans": config.GCP_BQ_TABLE_LANGUAGE_TRANS,
                "staging_review": config.GCP_BQ_TABLE_VOE,
                "staging_review_stats": config.GCP_BQ_TABLE_VOE_REVIEW_STATS,
                "staging_custom_dimension_review": config.GCP_BQ_TABLE_VOE_CUSTOM_DIMENSION,
                "table_casestudy_custom_dimension_statistics": config.GCP_BQ_TABLE_VOE_CASESTUDY_CUSTOM_DIMENSION_STATISTICS,
                "table_company_aliases": config.GCP_BQ_TABLE_VOE_COMPANY_ALIASES_LIST,
                "table_parent_review_mapping": config.GCP_BQ_TABLE_VOE_PARENT_REVIEW_MAPPING,
                # if a VOE chart needs data from a materialized table for optimization,
                # 1. table key and id need to be declared (here)
                # 2. an insert function (Bigquery SQL) needs to be constructed (at the end)
                # 3. code for table must be added to export_all_chart function, VoE part (below)
                "table_11_termcount_table": "VOE_11_termcount_table",
                "table_11_1_keywordcount_table": "VOE_11_1_keywordcount_table",
                "table_5_heatmapdim_table": "VOE_5_heatmapdim_table",
                "table_3_ratingtime_table": "VOE_3_ratingtime_table",
                "table_6_5_competitor_table": "VOE_6_5_competitor_table",
            }

            moving_staging_to_next_step_for_review_and_job(
                payload, bqclient, table_map, run_id
            )

            # update progress
            utils.update_progress(
                payload=payload,
                worker_progress=0.4,
                project_id=config.GCP_PROJECT_ID,
                progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                logger=logger
            )

            if is_exist_data_type(
                payload["payload"]["company_datasources"],
                etl_const.Meta_DataType.JOB.value,
            ):
                moving_staging_to_next_step_for_job(
                    payload, bqclient, table_map, run_id
                )

                # update progress
                utils.update_progress(
                    payload=payload,
                    worker_progress=0.6,
                    project_id=config.GCP_PROJECT_ID,
                    progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                    logger=logger
                )

            if is_exist_data_type(
                payload["payload"]["company_datasources"],
                etl_const.Meta_DataType.REVIEW.value,
            ):
                moving_staging_to_next_step_for_review(
                    payload, bqclient, table_map, run_id
                )

                # update progress
                utils.update_progress(
                    payload=payload,
                    worker_progress=0.8,
                    project_id=config.GCP_PROJECT_ID,
                    progress_topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                    logger=logger
                )

            # export all chart
            logger.info(f"Begin export_all_chart VoE")
            export_all_chart(bqclient, payload, run_id, table_map)

        else:
            raise Exception(
                f"Not support nlp_type={payload['payload']['dimension_config']['nlp_type']}"
            )

        time.sleep(30)
        pubsub_util.publish(
            logger,
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
            {
                "event": "finish",
                "type": "export",
                "run_id": run_id,
                "postprocess": payload["postprocess"],
                "case_study_id": payload["case_study_id"],
                "request_id": payload["request_id"],
                "error": "",
            },
        )
        set_terminator(None)

    except Exception as error:
        time.sleep(30)
        pubsub_util.publish(
            logger,
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
            {
                "event": "fail",
                "type": "export",
                "run_id": run_id,
                "postprocess": payload["postprocess"],
                "case_study_id": payload["case_study_id"],
                "request_id": payload["request_id"],
                "error": str(error),
            },
        )
        logger.exception(f"Error: {error}")


def is_exist_data_type(company_datasources, data_type):
    for company in company_datasources:
        company_urls = company["urls"]
        for url_dict in company_urls:
            if "type" in url_dict and url_dict["type"].lower() == data_type.lower():
                return True
    return False


def export_all_chart(bqclient, payload, run_id, table_map):

    # moving data to VOC tables
    if payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoC".lower():
        # TEMP RESOLUTION: only move data if nlp type is VOC
        # TODO: separate MOVING DATA to another function
        logger.info(f"[MOVING_DATA] insert_voc_6_7: {payload['case_study_id']}")
        if not insert_voc_6_7(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_6_7_commonterms_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voc_6_7_1: {payload['case_study_id']}")
        if not insert_voc_6_7_1(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_6_7_1_commonkeywords_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voc_6_5: {payload['case_study_id']}")
        if not insert_voc_6_5(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_6_5_competitor_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voc_5: {payload['case_study_id']}")
        if not insert_voc_5(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_5_heatmapdim_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voc_3: {payload['case_study_id']}")
        if not insert_voc_3(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_3_ratingtime_table"],
            table_map["table_batch_status"],
            table_map["table_casestudy_batchid"],
            table_map["table_parent_review_mapping"],
        ):
            return None

    # moving data to VOE tables
    if payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoE".lower():
        logger.info(f"[MOVING_DATA] insert_voe_5: {payload['case_study_id']}")
        if not insert_voe_5(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_5_heatmapdim_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voe_11: {payload['case_study_id']}")
        if not insert_voe_11(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_11_termcount_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voe_11_1: {payload['case_study_id']}")
        if not insert_voe_11_1(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_11_1_keywordcount_table"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voe_3: {payload['case_study_id']}")
        if not insert_voe_3(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_3_ratingtime_table"],
            table_map["table_batch_status"],
            table_map["table_casestudy_batchid"],
            table_map["table_parent_review_mapping"],
        ):
            return None

        logger.info(f"[MOVING_DATA] insert_voe_6_5: {payload['case_study_id']}")
        if not insert_voe_6_5(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_6_5_competitor_table"],
        ):
            return None

    meta_data = {}
    for export_item in payload["payload"]["exports"]:
        chart_code = export_item["code"]

        # only export chart codes that are in meta_data
        if chart_code in metadata.keys():
            chart_meta_data = {
                "datamart_table": export_item["table"],
                "prefix_filename": export_item["prefix"],
            }
            # meta_data[chart_code](chart_meta_data)
            meta_data[chart_code] = chart_meta_data
            bigquery_helpers.export_chart(bqclient, chart_code, payload, meta_data, logger)


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


# support serialize datetime
def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


def insert_parent_review_mapping(logger, bqclient, payload):
    """
    Extract mapping between parent_review_id and review_id (children) from either staging.voc or voe
    and insert into respective table: staging.parent_review_mapping (voc) or staging.voe_parent_review_mapping
    """
    try:
        # prepare
        case_study_id = payload["case_study_id"]
        request_id = payload["request_id"]
        if payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoC".lower():
            staging_table_id = config.GCP_BQ_TABLE_VOC
            parent_review_mapping_table_id = config.GCP_BQ_TABLE_PARENT_REVIEW_MAPPING
        else:
            staging_table_id = config.GCP_BQ_TABLE_VOE
            parent_review_mapping_table_id = (
                config.GCP_BQ_TABLE_VOE_PARENT_REVIEW_MAPPING
            )

        # check existence of mapping
        mapping_exists = bigquery_helpers._check_existing_parent_review_mappings(
            client=bqclient,
            case_study_id=case_study_id,
            parent_review_mapping_table_id=parent_review_mapping_table_id,
            logger=logger,
        )

        if mapping_exists:
            logger.info(
                f"Parent review id mapping exists for case study {case_study_id}, skipping"
            )
            return

        # extract and move mapping data from staging table to mapping table
        query = f"""
        INSERT INTO `{parent_review_mapping_table_id}`
        
        SELECT DISTINCT
            created_at,
            case_study_id,
            company_id,
            source_id,
            request_id,
            batch_id,
            file_name,
            parent_review_id,
            review_id,
            technical_type
        FROM `{staging_table_id}`
        WHERE case_study_id = {case_study_id}
        ;
        """
        query_job = bqclient.query(query)
        query_job.result()
        logger.info(
            f"Insert successful bigquery table_name={parent_review_mapping_table_id}"
        )

        return

    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_parent_review_mapping: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_review_country_mapping(logger, bqclient, payload):
    """
    Extract mapping between review_id and country from either staging.voc or voe
    and insert into respective table:
    - staging.review_country_mapping (voc) or
    - staging.voe_review_country_mapping (voe)
    """
    try:
        # prepare
        case_study_id = payload["case_study_id"]
        request_id = payload["request_id"]
        if payload["payload"]["dimension_config"]["nlp_type"].lower() == "VoC".lower():
            staging_table_id = config.GCP_BQ_TABLE_VOC
            review_country_mapping_table_id = config.GCP_BQ_TABLE_REVIEW_COUNTRY_MAPPING
        else:
            staging_table_id = config.GCP_BQ_TABLE_VOE
            review_country_mapping_table_id = (
                config.GCP_BQ_TABLE_VOE_REVIEW_COUNTRY_MAPPING
            )

        # check existence of mapping
        mapping_exists = bigquery_helpers._check_existing_review_country_mapping(
            client=bqclient,
            case_study_id=case_study_id,
            review_country_mapping_table_id=review_country_mapping_table_id,
            logger=logger,
        )

        if mapping_exists:
            logger.info(
                f"Review country mapping exists for case study {case_study_id}, skipping"
            )
            return

        # extract and move mapping data from staging table to mapping table
        query = f"""
        INSERT INTO `{review_country_mapping_table_id}` (
            created_at,
            case_study_id,
            company_id,
            source_id,
            request_id,
            batch_id,
            file_name,
            review_id,
            review_country,
            country_code
        )
        
        SELECT DISTINCT
            created_at,
            case_study_id,
            company_id,
            source_id,
            request_id,
            batch_id,
            file_name,
            review_id,
            review_country,
            country_code
        FROM `{staging_table_id}`
        WHERE case_study_id = {case_study_id}
        ;
        """
        query_job = bqclient.query(query)
        query_job.result()
        logger.info(
            f"Insert successful bigquery table_name={review_country_mapping_table_id}"
        )

        return True

    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_review_country_mapping: "
            f"case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def moving_staging_to_next_step_for_job(payload, bqclient, table_map, run_id):
    try:
        # Step 5: Update status of old batch_id to 'Inactive' in table batch_status in staging
        # logger.info(f"[MOVING_DATA] update_batch_status_before_insert: {payload['case_study_id']}")
        # if not update_batch_status_before_insert(payload['case_study_id'], table_map['table_batch_status'], table_map['table_casestudy_company_source'], table_map['table_casestudy_batchid']):
        #     return None
        # Step 6: Insert new data into table batch_status in staging
        logger.info(
            f"[MOVING_DATA] insert_batch_status_before_moving_data: {payload['case_study_id']}"
        )
        if not insert_batch_status_before_moving_data(
            bqclient,
            payload["case_study_id"],
            run_id,
            table_map["table_batch_status"],
            table_map["table_casestudy_batchid"],
        ):
            return None

        time_to_sleep = 60
        logger.info(f"Sleep: {time_to_sleep} seconds")
        time.sleep(time_to_sleep)

        # Step 8: Insert into table summary_table in dwh
        if not delete_summary_table_job(bqclient, payload["case_study_id"]):
            return None
        if not create_summary_table_job(bqclient, payload["case_study_id"]):
            return None
        logger.info(
            f"[MOVING_DATA] insert_summary_table_job: {payload['case_study_id']}"
        )
        if not insert_summary_table_job(
            bqclient, int(payload["case_study_id"]), run_id
        ):
            return None
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] moving_staging_to_next_step_for_job: case_study_id={payload['case_study_id']},error={error.__class__.__name__}"
        )
        raise error


def moving_staging_to_next_step_for_review(payload, bqclient, table_map, run_id):
    try:
        request_payload = payload["payload"]
        for polarity_item in request_payload["polarities"]:
            bigquery_helpers.insert_polarity_trans(
                logger=logger,
                bqclient=bqclient,
                case_study_id=payload["case_study_id"],
                case_study_name=request_payload["case_study_name"],
                nlp_polarity=polarity_item["nlp_polarity"],
                modified_polarity=polarity_item["modified_polarity"],
                run_id=run_id,
                table_polarity_trans=table_map["table_polarity_trans"],
            )

        # insert to dimension_keyword_list
        if (
            "keywords_include" in request_payload["dimension_config"]
            and "keywords_exclude" in request_payload["dimension_config"]
        ):
            bigquery_helpers.insert_dimension_keyword_list(
                bqclient,
                payload["case_study_id"],
                request_payload["dimension_config"]["keywords_include"],
                request_payload["dimension_config"]["keywords_exclude"],
                run_id,
                table_map["table_dimension_keyword_list"],
                logger=logger
            )
        else:
            bigquery_helpers.insert_dimension_keyword_list(
                bqclient,
                payload["case_study_id"],
                "",
                "",
                run_id,
                table_map["table_dimension_keyword_list"],
                logger=logger
            )

        # insert to company_alias
        bigquery_helpers.insert_company_aliases(
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            companies_data_payload=request_payload["company_datasources"],
            run_id=run_id,
            table_company_aliases_list=table_map["table_company_aliases"],
            logger=logger
        )

        # insert to parent_review_mapping for technical review support
        insert_parent_review_mapping(logger=logger, bqclient=bqclient, payload=payload)
        # insert to review_country_mapping for review country support
        insert_review_country_mapping(logger=logger, bqclient=bqclient, payload=payload)

        time_to_sleep = 60
        logger.info(f"Sleep: {time_to_sleep} seconds")
        time.sleep(time_to_sleep)

        # Step 5: Update status of old batch_id to 'Inactive' in table batch_status in staging
        # logger.info(f"[MOVING_DATA] __update_batch_status_before_insert: {payload['case_study_id']}")
        # if not update_batch_status_before_insert(int(payload['case_study_id']), table_map['table_batch_status'],
        #         table_map['table_casestudy_company_source'], table_map['table_casestudy_batchid']):
        #     return None
        # Step 6: Insert new data into table batch_status in staging
        logger.info(
            f"[MOVING_DATA] __insert_batch_status_before_moving_data: {payload['case_study_id']}"
        )
        if not insert_batch_status_before_moving_data(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_batch_status"],
            table_map["table_casestudy_batchid"],
        ):
            return None

        # Step 7: Insert into table nlp_output_case_study in staging
        logger.info(
            f"[MOVING_DATA] __insert_nlp_output_case_study: {payload['case_study_id']}"
        )
        if not insert_nlp_output_case_study(
            bqclient=bqclient,
            case_study_id=int(payload["case_study_id"]),
            run_id=run_id,
            table_nlp_output_case_study=table_map["table_nlp_output_case_study"],
            table_batch_status=table_map["table_batch_status"],
            table_casestudy_company_source=table_map["table_casestudy_company_source"],
            table_voc=table_map["staging_review"],
            table_casestudy_batchid=table_map["table_casestudy_batchid"],
            table_dimension_keyword_list=table_map["table_dimension_keyword_list"],
            table_custom_dimension_review_data=table_map[
                "staging_custom_dimension_review"
            ],
            table_review_stats=table_map["staging_review_stats"],
        ):
            return None
        # Step 8: Insert into table summary_table in dwh
        if not delete_summary_table(
            bqclient,
            int(payload["case_study_id"]),
            table_map["table_summary_table_prefix"],
        ):
            return None
        if not create_summary_table(
            bqclient, payload["case_study_id"], table_map["table_summary_table_prefix"]
        ):
            return None
        if not insert_summary_table(
            bqclient,
            int(payload["case_study_id"]),
            run_id,
            table_map["table_summary_table_prefix"],
            table_map["table_casestudy_dimension_config"],
            table_map["table_nlp_output_case_study"],
            table_map["table_polarity_trans"],
            table_map["table_language_trans"],
            table_map["table_casestudy_company_source"],
        ):
            return None
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] moving_staging_to_next_step_for_review: case_study_id={payload['case_study_id']},error={error.__class__.__name__}"
        )
        raise error


def moving_staging_to_next_step_for_review_and_job(
    payload, bqclient, table_map, run_id
):
    try:
        request_id = payload["request_id"]
        request_payload = payload["payload"]
        dimension_config_id = request_payload["dimension_config"]["version_id"]
        dimension_config_name = request_payload["dimension_config"]["name"]
        company_batch_items = payload["company_batch_items"]

        logger.info(f"Insert insert_casestudy_run_id")
        bigquery_helpers.insert_case_study_run_id(
            logger=logger,
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            run_id=run_id,
            table_name=table_map["table_casestudy_run_id"],
        )

        created_at = dt.datetime.now().isoformat()
        for company_batch_item in company_batch_items:
            logger.info(
                f"Insert casestudy_dimension_config batch_id={company_batch_item['batch_id']}"
            )
            bigquery_helpers.insert_casestudy_batchid(
                logger=logger,
                bqclient=bqclient,
                case_study_id=payload["case_study_id"],
                request_id=request_id,
                batch_id=company_batch_item["batch_id"],
                batch_name=company_batch_item["batch_name"],
                company_id=company_batch_item["company_id"],
                source_id=company_batch_item["source_id"],
                created_at=created_at,
                run_id=run_id,
                table_name=table_map["table_casestudy_batchid"],
            )
            time.sleep(5)

        if (
            "clone_from_cs_id" in request_payload
            and request_payload["clone_from_cs_id"]
        ):
            clone_casestudy_batchid(
                logger=logger,
                bqclient=bqclient,
                request_id=request_id,
                case_study_id=payload["case_study_id"],
                clone_from_cs_id=request_payload["clone_from_cs_id"],
                run_id=run_id,
                table_casestudy_batchid_name=table_map["table_casestudy_batchid"],
                table_casestudy_run_id_name=table_map["table_casestudy_run_id"],
            )

        logger.info(f"Insert casestudy_dimension_config")
        bigquery_helpers.insert_casestudy_dimension_config(
            logger=logger,
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            case_study_name=request_payload["case_study_name"],
            dimension_config_id=dimension_config_id,
            run_id=run_id,
            payload=request_payload,
            table_casestudy_dimension_config_name=table_map[
                "table_casestudy_dimension_config"
            ],
        )
        logger.info(f"Insert casestudy_company_source")
        bigquery_helpers.insert_casestudy_company_source(
            logger=logger,
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            case_study_name=request_payload["case_study_name"],
            companies=request_payload["company_datasources"],
            dimension_config_id=dimension_config_id,
            run_id=run_id,
            request_payload=request_payload,
            table_casestudy_company_source=table_map["table_casestudy_company_source"],
        )
        logger.info(f"Insert casestudy_custom_dimension_statistics")
        bigquery_helpers.insert_table_casestudy_custom_dimension_statistics(
            logger=logger,
            bqclient=bqclient,
            case_study_id=payload["case_study_id"],
            case_study_name=request_payload["case_study_name"],
            dimension_config_id=dimension_config_id,
            dimension_config_name=dimension_config_name,
            run_id=run_id,
            table_staging_custom_dimension_review=table_map[
                "staging_custom_dimension_review"
            ],
            table_casestudy_dimension_config=table_map[
                "table_casestudy_dimension_config"
            ],
            table_casestudy_custom_dimension_statistics=table_map[
                "table_casestudy_custom_dimension_statistics"
            ],
            table_batch_status=table_map["table_batch_status"],
            table_casestudy_batchid=table_map["table_casestudy_batchid"],
        )
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] moving_staging_to_next_step_for_review: case_study_id={payload['case_study_id']},error={error.__class__.__name__}"
        )
        raise error


def clone_casestudy_batchid(
    logger,
    bqclient,
    request_id,
    case_study_id,
    clone_from_cs_id,
    run_id,
    table_casestudy_batchid_name,
    table_casestudy_run_id_name,
):
    try:
        sql = """
        INSERT INTO `{table_casestudy_batchid}`
        WITH RUN_IDS AS (
            SELECT * FROM `{table_case_study_run_id}` WHERE case_study_id = {clone_from_cs_id} ORDER BY created_at DESC LIMIT 1
        )

        SELECT 
            created_at, {case_study_id} AS case_study_id, {request_id} AS request_id, 
            batch_id, batch_name, company_id, source_id, '{run_id}' AS run_id
        FROM `{table_casestudy_batchid}` AS BATCH_IDS 
        WHERE case_study_id = {clone_from_cs_id} AND BATCH_IDS.run_id IN (SELECT run_id FROM RUN_IDS);
        """.format(
            table_casestudy_batchid=table_casestudy_batchid_name,
            table_case_study_run_id=table_casestudy_run_id_name,
            request_id=request_id,
            case_study_id=case_study_id,
            run_id=run_id,
            clone_from_cs_id=clone_from_cs_id,
        )

        query_job = bqclient.query(sql)
        query_job.result()  # Wait for the job to complete.
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] clone_casestudy_batchid: case_study_id={case_study_id}, clone_id={clone_from_cs_id}, error={error.__class__.__name__}"
        )
        return False


def update_batch_status_before_insert(
    case_study_id,
    bqclient,
    table_batch_status,
    table_casestudy_company_source,
    table_casestudy_batchid,
):
    try:
        sql = """
        UPDATE `{table_batch_status}`
        SET status = 'Inactive' 
        WHERE batch_id in (SELECT batch_id FROM `{table_casestudy_batchid}` WHERE case_study_id = {case_study_id});
        """.format(
            table_batch_status=table_batch_status,
            table_casestudy_company_source=table_casestudy_company_source,
            table_casestudy_batchid=table_casestudy_batchid,
            case_study_id=case_study_id,
        )

        query_job = bqclient.query(sql)
        query_job.result()  # Wait for the job to complete.
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __update_batch_status_before_insert: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_batch_status_before_moving_data(
    bqclient, case_study_id, run_id, table_batch_status, table_casestudy_batchid
):
    try:
        sql = """
        INSERT INTO `{table_batch_status}`
        
        SELECT a.created_at, a.batch_id, a.batch_name, a.source_id, CAST(NULL AS STRING), a.company_id, CAST(NULL AS STRING), CAST('Active' AS STRING), CAST(null as int64)
        FROM `{table_casestudy_batchid}` a
        WHERE run_id = '{run_id}' AND case_study_id = {case_study_id};
        """.format(
            table_batch_status=table_batch_status,
            table_casestudy_batchid=table_casestudy_batchid,
            run_id=run_id,
            case_study_id=case_study_id,
        )
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_batch_status_before_moving_data: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_nlp_output_case_study(
    bqclient,
    case_study_id,
    run_id,
    table_nlp_output_case_study,
    table_batch_status,
    table_casestudy_company_source,
    table_voc,
    table_casestudy_batchid,
    table_dimension_keyword_list,
    table_custom_dimension_review_data,
    table_review_stats,
):
    try:
        sql = """
        INSERT INTO `{table_nlp_output_case_study}`

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
        review_stats_data AS(
        SELECT *
        FROM {table_review_stats}
        WHERE
            batch_id IN (
                SELECT
                    batch_id FROM BATCH_LIST
            )
            AND case_study_id = {case_study_id}
        ),
        custom_dimension_review_data AS (
            SELECT
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
                case_study_id,
                review_country,
                parent_review_id,
                technical_type,
                country_code,
                CAST(company_datasource_id AS INT64) company_datasource_id,
                url
            FROM
                `{table_voc_custom_dimension}` a
            WHERE
                a.batch_id IN (
                    SELECT
                        batch_id FROM BATCH_LIST
                )
                AND a.case_study_id = {case_study_id}
                AND a.run_id = '{run_id}'
        ), 
        review_data AS (
            SELECT
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
                case_study_id,
                review_country,
                parent_review_id,
                technical_type,
                country_code,
                CAST(company_datasource_id AS INT64) company_datasource_id,
                url
            FROM
                `{table_voc}` a
            WHERE
                a.batch_id IN (
                    SELECT
                        batch_id FROM BATCH_LIST
                )
                AND a.case_study_id = {case_study_id}
        ),
        union_review_data AS (
            SELECT *
            FROM review_data
            UNION ALL
            SELECT * 
            FROM custom_dimension_review_data
        ),
        full_review_data AS (
            SELECT 
                rd.*,
                sd.total_reviews,
                sd.total_ratings,
                sd.average_rating
            FROM union_review_data rd
            LEFT JOIN review_stats_data sd
                ON rd.company_id = sd.company_id
                AND rd.source_id = sd.source_id
                AND rd.url = sd.url
        ),
        TEMP1 AS (
            SELECT
                b.created_at,
                review_id,
                a.source_name,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                a.language,
                a.review,
                a.trans_review,
                a.trans_status,
                a.code,
                a.dimension,
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
                b.company_id,
                b.source_id,
                a.step_id,
                request_id,
                b.is_target,
                b.case_study_id,
                b.case_study_name,
                b.dimension_config_id,
                b.run_id,
                a.review_country,
                a.parent_review_id,
                a.technical_type,
                a.country_code,
                a.company_datasource_id,
                a.url,
                a.total_reviews,
                a.total_ratings,
                a.average_rating
            FROM
                full_review_data a
                LEFT JOIN (
                    SELECT
                        created_at,
                        case_study_id,
                        case_study_name,
                        dimension_config_id,
                        company_id,
                        company_name,
                        source_name,
                        source_id,
                        nlp_pack,
                        nlp_type,
                        is_target,
                        run_id
                    FROM
                        `{table_casestudy_company_source}`
                    WHERE
                        case_study_id = {case_study_id}
                        AND run_id = '{run_id}' 
                ) b 
                ON a.company_id = b.company_id
                AND a.source_id = b.source_id
                AND a.nlp_pack = b.nlp_pack
                AND a.nlp_type = b.nlp_type
                AND a.case_study_id = b.case_study_id 
        ),
        in_keyword_table AS (
            SELECT *
            FROM  `{table_dimension_keyword_list}`, UNNEST(split(in_list, '\t')) as in_keyword  
            WHERE in_list is not null 
                AND case_study_id = {case_study_id}
                AND run_id = '{run_id}'     
            UNION ALL           
                SELECT *, CAST(NULL AS STRING) AS in_keyword 
            FROM `{table_dimension_keyword_list}` 
            WHERE in_list is null 
                AND case_study_id = {case_study_id}
                AND run_id = '{run_id}'
        ),
        ex_keyword_table AS (
            SELECT *
            FROM  `{table_dimension_keyword_list}`, UNNEST(split(ex_list, '\t')) as ex_keyword  
            WHERE 
                ex_list is not null 
                AND case_study_id = {case_study_id}
                AND run_id = '{run_id}' 
            UNION ALL
            SELECT *, CAST(NULL AS STRING) AS ex_keyword 
            FROM `{table_dimension_keyword_list}` 
            WHERE 
                ex_list is null
                AND case_study_id = {case_study_id}
                AND run_id = '{run_id}' 
        ),
        review_list as (
            SELECT 
                DISTINCT 
                review_id
                ,b.in_list 
                ,b.in_keyword
                ,REGEXP_CONTAINS( lower(trans_review),CONCAT(r'\\b', lower(in_keyword), r'\\b')) as in_is_match
                ,c.ex_list
                ,c.ex_keyword
                ,REGEXP_CONTAINS( lower(trans_review),CONCAT(r'\\b', lower(ex_keyword), r'\\b')) as ex_is_match

            FROM ( SELECT DISTINCT * FROM TEMP1 ) a
            CROSS JOIN (
                SELECT * 
                FROM in_keyword_table
            ) b 
            CROSS JOIN (
                SELECT * 
                FROM ex_keyword_table
            ) c
        ),
        review_final AS (
            SELECT a.* 
            FROM ( SELECT DISTINCT * FROM TEMP1) a
            WHERE 
                review_id in (
                    SELECT DISTINCT review_id FROM review_list 
                    WHERE
                    CASE 
                        WHEN (in_list IS NOT NULL AND in_list != '' ) THEN in_is_match = true
                    ELSE 1=1 END
                )
                AND review_id not in (
                    SELECT DISTINCT review_id 
                    FROM review_list 
                    WHERE (ex_list IS NOT NULL AND ex_list != '')
                    AND ex_is_match = true
                ) 
        )
        SELECT 
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
            CASE 
                WHEN technical_type= "pros" AND polarity in ('N','N+','NEU') THEN 'P'
                WHEN technical_type= "cons" AND polarity in ('NEU','P','P+') THEN 'N'
                WHEN technical_type= "problems_solved" AND polarity in ('N','N+','NEU') THEN 'P'
                WHEN technical_type= "switch_reasons" AND polarity in ('N','N+','NEU') THEN 'P'
                ELSE polarity
            END AS polarity ,
            rating,
            batch_id,
            batch_name,
            file_name,
            review_date,
            company_id,
            source_id,
            step_id,
            request_id,
            is_target,
            case_study_id,
            case_study_name,
            dimension_config_id,
            run_id,
            parent_review_id,
            technical_type,
            company_datasource_id,
            url,
            review_country,
            country_code,
            total_reviews,
            total_ratings,
            average_rating
        FROM review_final
        ;
        """.format(
            table_batch_status=table_batch_status,
            table_voc=table_voc,
            table_voc_custom_dimension=table_custom_dimension_review_data,
            table_casestudy_company_source=table_casestudy_company_source,
            table_nlp_output_case_study=table_nlp_output_case_study,
            table_casestudy_batchid=table_casestudy_batchid,
            run_id=run_id,
            table_dimension_keyword_list=table_dimension_keyword_list,
            case_study_id=case_study_id,
            table_review_stats=table_review_stats,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_nlp_output_case_study: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def create_summary_table(bqclient, case_study_id, table_summary_table_prefix):
    table_id = None
    try:
        table_id = f"{table_summary_table_prefix}_{case_study_id}"
        sql = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        ( 
            created_at	timestamp	,
            case_study_id	int64	,
            case_study_name	string	,
            source_id	int64	,
            source_name	string	,
            company_id	int64	,
            company_name	string	,
            nlp_pack	string	,
            nlp_type	string	,
            user_name	string	,
            dimension_config_id	int64	,
            dimension_config_name	string	,
            review_id	string	,
            review	string	,
            trans_review	string	,
            trans_status	string	,
            review_date	date	,
            rating	float64	,
            language_code	string	,
            language	string	,
            code	string	,
            dimension_type	string	,
            dimension	string	,
            modified_dimension	string	,
            label	string	,
            modified_label	string	,
            is_used	bool	,
            terms	string	,
            relevance	float64	,
            rel_relevance	float64	,
            polarity	string	,
            modified_polarity	float64	,
            batch_id	int64	,
            batch_name	string	,
            run_id	string	,
            abs_relevance_inf	float64	,
            rel_relevance_inf	float64	,
            parent_review_id    string ,
            technical_type  string ,
            company_datasource_id   int64 ,
            url string ,
            review_country  string ,
            country_code    string ,
            total_reviews   int64 ,
            total_ratings   int64 ,
            average_rating  float64
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        """
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] create_summary_table: case_study_id={case_study_id},error={error.__class__.__name__},table={table_id}"
        )
        raise error


def create_summary_table_job(bqclient, case_study_id):
    table_id = None
    try:
        table_id = f"{config.GCP_BQ_TABLE_SUMMARY_TABLE_VOE_JOB_PREFIX}_{case_study_id}"
        sql = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        ( 
            created_at  timestamp,
            case_study_id int64,
            case_study_name string,
            nlp_pack string,
            nlp_type string,
            dimension_config_id int64,
            dimension_config_name string,
            source_name string,
            source_id int64,
            company_name string,
            company_id int64,
            job_id string,
            job_name string,
            job_function string,
            job_type string,
            posted_date timestamp,
            job_country string,
            fte float64,
            role_seniority string,    
            batch_id int64,
            batch_name string,
            run_id string
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1)) ;
        """
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] create_summary_table: case_study_id={case_study_id},error={error.__class__.__name__},table={table_id}"
        )
        raise error


def delete_summary_table(bqclient, case_study_id, table_summary_table_prefix):
    logger.info(f"[MOVING_DATA] delete_summary_table: {case_study_id}")
    table_id = None
    try:
        table_id = f"{table_summary_table_prefix}_{case_study_id}"
        bqclient.delete_table(table_id, not_found_ok=True)
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] delete_summary_table: case_study_id={case_study_id},error={error.__class__.__name__},table={table_id}"
        )
        raise error


def delete_summary_table_job(bqclient, case_study_id):
    logger.info(f"[MOVING_DATA] delete_summary_table_job: {case_study_id}")
    table_id = None
    try:
        table_id = f"{config.GCP_BQ_TABLE_SUMMARY_TABLE_VOE_JOB_PREFIX}_{case_study_id}"
        bqclient.delete_table(table_id, not_found_ok=True)
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] delete_summary_table_voe: case_study_id={case_study_id},error={error.__class__.__name__},table={table_id}"
        )
        raise error


def insert_summary_table(
    bqclient,
    case_study_id,
    run_id,
    table_summary_table_prefix,
    table_casestudy_dimension_config,
    table_nlp_output_case_study,
    table_polarity_trans,
    table_language_trans,
    table_casestudy_company_source,
):
    logger.info(f"[MOVING_DATA] insert_summary_table: {case_study_id}")
    try:
        sql = """
        INSERT INTO `{table_summary_table}` 

        WITH DIM_LIST as (
            SELECT nlp_dimension as dimension, nlp_label as label 
            FROM `{table_casestudy_dimension_config}` 
            WHERE case_study_id = {case_study_id}
                AND run_id = '{run_id}'
        ), TEMP1 as (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                c.dimension_type,
                a.dimension,
                c.modified_dimension,
                # CASE WHEN c.nlp_dimension is null THEN a.dimension ELSE c.modified_dimension END modified_dimension,
                a.label,
                c.modified_label,
                # CASE WHEN c.nlp_label is null THEN a.label ELSE c.modified_label END modified_label,
                c.is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id,
                a.parent_review_id,
                a.technical_type,
                a.company_datasource_id,
                a.url,
                a.review_country,
                a.country_code,
                a.total_reviews,
                a.total_ratings,
                a.average_rating
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, nlp_dimension, modified_dimension, nlp_label, modified_label,
                    is_used, dimension_type
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
                AND  a.dimension = c.nlp_dimension
                AND a.label = c.nlp_label
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id = '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
            WHERE a.case_study_id = {case_study_id}
                AND a.dimension in (SELECT dimension FROM DIM_LIST) AND a.label in (SELECT label FROM DIM_LIST) 
                AND run_id = '{run_id}'            
        ) ,TEMP2 AS (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                c.dimension_type,
                a.dimension,
                c.modified_dimension,
                a.label,
                a.label as modified_label,
                cast(null as bool) as is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id,
                a.parent_review_id,
                a.technical_type,
                a.company_datasource_id,
                a.url,
                a.review_country,
                a.country_code,
                a.total_reviews,
                a.total_ratings,
                a.average_rating
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, is_used, dimension_type, nlp_dimension, modified_dimension
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
                AND a.dimension = c.nlp_dimension
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id =  '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
                            
            WHERE a.case_study_id = {case_study_id}
                AND a.dimension in (SELECT dimension FROM DIM_LIST) and a.label not in (SELECT label FROM DIM_LIST)
                AND run_id =  '{run_id}'
        ) ,TEMP3 AS (
            SELECT
                a.created_at,
                a.case_study_id,
                a.case_study_name,
                a.source_id,
                a.source_name,
                a.company_id,
                a.company_name,
                a.nlp_pack,
                a.nlp_type,
                a.user_name,
                c.dimension_config_id,
                c.dimension_config_name,
                a.review_id,
                a.review,
                a.trans_review,
                a.trans_status,
                date(a.review_date) review_date,
                a.rating,
                a.language as language_code,
                e.language,
                a.code,
                cast(null as string) as dimension_type,
                a.dimension,
                a.dimension as modified_dimension,
                a.label,
                a.label as modified_label,
                cast(null as bool) as is_used,
                a.terms,
                a.relevance,
                a.rel_relevance,
                a.polarity,
                d.modified_polarity,
                a.batch_id,
                a.batch_name,
                a.run_id,
                a.parent_review_id,
                a.technical_type,
                a.company_datasource_id,
                a.url,
                a.review_country,
                a.country_code,
                a.total_reviews,
                a.total_ratings,
                a.average_rating
            FROM `{table_nlp_output_case_study}` a
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_pack, nlp_type, dimension_config_id,
                    dimension_config_name, is_used, dimension_type
                FROM `{table_casestudy_dimension_config}`
                WHERE run_id = '{run_id}'
            ) c
            ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
            LEFT JOIN (
                SELECT created_at, case_study_id, case_study_name, nlp_polarity, modified_polarity
                FROM `{table_polarity_trans}`
                WHERE run_id =  '{run_id}'
            ) d  
            ON a.case_study_id = d.case_study_id
                AND a.polarity = d.nlp_polarity
            LEFT JOIN `{table_language_trans}` e
                ON a.language = e.language_code
                            
            WHERE a.case_study_id = {case_study_id}
                AND (a.dimension is null OR a.dimension not in (SELECT dimension FROM DIM_LIST))
                AND run_id =  '{run_id}'
        ), TOTAL AS (
            SELECT distinct * FROM TEMP1 
            UNION  ALL
            SELECT distinct * FROM TEMP2
            UNION  ALL
            SELECT distinct * FROM TEMP3
        )

        SELECT
            created_at,
            a.case_study_id,
            case_study_name,
            source_id,
            source_name,
            company_id,
            company_name,
            nlp_pack,
            nlp_type,
            user_name,
            dimension_config_id,
            dimension_config_name,
            review_id,
            review,
            trans_review,
            trans_status,
            review_date,
            rating,
            CASE 
                WHEN language_code is null or language_code = '' THEN 'blank'
                ELSE language_code
            END as language_code,
            CASE 
                WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
                WHEN language is null AND language_code is not null THEN language_code
                ELSE language
            END as language,
            code,
            dimension_type,
            dimension,
            modified_dimension,
            label,
            modified_label,
            is_used,
            terms,
            a.relevance,
            a.rel_relevance,
            polarity,
            modified_polarity,
            batch_id,
            batch_name,
            a.run_id,
            abs_relevance_inf,
            rel_relevance_inf,
            a.parent_review_id,
            a.technical_type,
            a.company_datasource_id,
            a.url,
            a.review_country,
            a.country_code,
            a.total_reviews,
            a.total_ratings,
            a.average_rating
        FROM (SELECT DISTINCT * FROM TOTAL) a
        LEFT JOIN (
            SELECT 
                DISTINCT 
                case_study_id,
                run_id, 
                abs_relevance_inf, 
                rel_relevance_inf 
            FROM `{table_casestudy_company_source}` a
            WHERE a.case_study_id = {case_study_id} AND run_id =  '{run_id}'
        ) b
        ON a.case_study_id = b.case_study_id AND a.run_id = b.run_id
        WHERE
            (CASE WHEN abs_relevance_inf > 0 THEN a.relevance >= abs_relevance_inf ELSE 1=1 END) AND 
            (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END)
        ;

        """.format(
            table_summary_table=f"{table_summary_table_prefix}_{case_study_id}",
            table_casestudy_dimension_config=table_casestudy_dimension_config,
            case_study_id=case_study_id,
            run_id=run_id,
            table_nlp_output_case_study=table_nlp_output_case_study,
            table_polarity_trans=table_polarity_trans,
            table_language_trans=table_language_trans,
            table_casestudy_company_source=table_casestudy_company_source,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_summary_table: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_summary_table_job(bqclient, case_study_id, run_id):
    logger.info(f"[MOVING_DATA] insert_summary_table_job: {case_study_id}")
    try:
        sql = """
        INSERT INTO `{table_voe_job_summary_table}` 
        WITH BATCH_LIST AS (
                SELECT
                    batch_id
                FROM
                    `{table_voe_batch_status}`
                WHERE
                    batch_id in (
                        SELECT
                            batch_id
                        FROM
                            `{table_voe_casestudy_batchid}`
                        WHERE
                            case_study_id = {case_study_id}
                    )
                    AND status = 'Active'
            ),
            TEMP1 AS (
                SELECT
                    a.created_at,
                    a.case_study_id,
                    c.case_study_name,
                    c.nlp_pack,
                    c.nlp_type,
                    c.dimension_config_id,
                    a.source_name,
                    a.source_id,
                    a.company_name,
                    a.company_id,
                    a.job_id,
                    a.job_name,
                    a.job_function,
                    a.job_type,
                    a.posted_date,
                    a.job_country,
                    b.fte,
                    a.role_seniority,
                    a.batch_id,
                    a.batch_name
                FROM
                    `{table_voe_job}` a
                    LEFT JOIN (
                        SELECT
                            DISTINCT case_study_id,
                            company_id,
                            company_name,
                            source_id,
                            source_name,
                            (max_fte + min_fte) / 2 AS fte,
                            batch_id
                        FROM
                            `{table_voe_company}` b
                        WHERE
                            case_study_id = {case_study_id}
                            AND batch_id IN ( 
                                SELECT DISTINCT FIRST_VALUE(batch_id) OVER ( 
                                    PARTITION BY case_study_id, company_id, source_id
                                    ORDER BY batch_id DESC
                                )
                                FROM `{table_voe_company}` 
                                WHERE case_study_id = {case_study_id}
                            )
                    ) b ON a.case_study_id = b.case_study_id
                    AND a.company_id = b.company_id
                    AND a.source_id = b.source_id
                    LEFT JOIN (
                        SELECT
                            DISTINCT created_at,
                            case_study_id,
                            case_study_name,
                            dimension_config_id,
                            company_id,
                            company_name,
                            source_name,
                            source_id,
                            nlp_pack,
                            nlp_type,
                            is_target,
                            run_id
                        FROM
                            `{table_voe_casestudy_company_source}`
                        WHERE
                            case_study_id = {case_study_id}
                            AND run_id = '{run_id}'
                    ) c ON a.case_study_id = c.case_study_id
                    AND a.company_id = c.company_id
                    AND a.source_id = c.source_id
                WHERE
                    a.batch_id IN (
                        SELECT
                            batch_id
                        FROM
                            BATCH_LIST
                    )
                    AND a.case_study_id = {case_study_id}
            ),
            VOE_JOB_COMPANY AS (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            DISTINCT *
                        FROM
                            TEMP1
                    )
            )
        SELECT
            d.created_at,
            d.case_study_id,
            d.case_study_name,
            d.nlp_pack,
            d.nlp_type,
            d.dimension_config_id,
            e.dimension_config_name,
            d.source_name,
            d.source_id,
            d.company_name,
            d.company_id,
            d.job_id,
            d.job_name,
            d.job_function,
            d.job_type,
            d.posted_date,
            d.job_country,
            d.fte,
            d.role_seniority,
            d.batch_id,
            d.batch_name,
            '{run_id}'
        FROM
            VOE_JOB_COMPANY d
            LEFT JOIN (
                SELECT
                    DISTINCT created_at,
                    case_study_id,
                    dimension_config_id,
                    dimension_config_name
                FROM
                    `{table_voe_casestudy_dimension_config}`
                WHERE
                    case_study_id = {case_study_id}
                    AND run_id = '{run_id}'
            ) e ON d.case_study_id = e.case_study_id
            AND d.dimension_config_id = e.dimension_config_id;
        """.format(
            table_voe_job_summary_table=f"{config.GCP_BQ_TABLE_SUMMARY_TABLE_VOE_JOB_PREFIX}_{case_study_id}",
            table_voe_batch_status=config.GCP_BQ_TABLE_VOE_BATCH_STATUS,
            table_voe_casestudy_batchid=config.GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID,
            table_voe_job=config.GCP_BQ_TABLE_VOE_JOB,
            table_voe_company=config.GCP_BQ_TABLE_VOE_COMPANY,
            table_voe_casestudy_company_source=config.GCP_BQ_TABLE_VOE_CASESTUDY_COMPANY_SOURCE,
            run_id=run_id,
            table_voe_casestudy_dimension_config=config.GCP_BQ_TABLE_VOE_CASESTUDY_DIMENSION_CONFIG,
            case_study_id=case_study_id,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_summary_table: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


# Moving data to VoC chart's table
def insert_voc_6_7(bqclient, case_study_id, run_id, table_6_7_commonterms_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{table_6_7_commonterms_table}` 

        WITH tmp AS (
            SELECT 
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_name,
                company_name,
                company_id, 
                source_id,
                review_id,
                review_date,
                modified_dimension ,
                modified_label,
                split(terms,'\t') AS single_terms,
                polarity,
                modified_polarity,
                run_id
            FROM `{prefix}datamart_cs.summary_table_*`
            WHERE _TABLE_SUFFIX = '{case_study_id}'
                and dimension is not null    
                and dimension_config_name is not null
                and is_used = true
                and run_id = '{run_id}'
            )
               
            ,single AS (SELECT  * EXCEPT(single_terms), single_terms FROM tmp, UNNEST(single_terms) AS single_terms)

            SELECT             
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                max(source_name) source_name,
                max(company_name) company_name,
                company_id, 
                source_id,
                # modified_dimension as dimension,
                polarity,
                lower(single_terms) as single_terms,
                review_date as daily_date,
                count(distinct review_id) as records,
                count(distinct review_id) as collected_review_count,
                run_id
            FROM single
            GROUP BY 
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id, 
                source_id,
                # modified_dimension,
                polarity,
                lower(single_terms),
                daily_date,
                run_id
        ;

        """.format(
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            table_6_7_commonterms_table=table_6_7_commonterms_table,
        )
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voc_6_7: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voc_6_7_1(
    bqclient, case_study_id, run_id, VOC_6_7_1_commonkeywords_table_id
):
    """
    Prepare and insert data into datamart table VOC 6.7.1 for given case study
    """
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = f"""
        INSERT INTO `{prefix}datamart.{VOC_6_7_1_commonkeywords_table_id}`  

        WITH voc_summary_table AS (
            SELECT DISTINCT
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_name,
                company_name,
                company_id, 
                source_id,
                polarity,
                review_date,
                review_id,
                run_id
            FROM `{prefix}datamart_cs.summary_table_*`
            WHERE _TABLE_SUFFIX = '{case_study_id}'
                AND run_id = '{run_id}'
                AND dimension IS NOT NULL 
                AND dimension_config_name IS NOT NULL 
                AND is_used = true
        ),
        tmp AS (SELECT *
            FROM `{prefix}staging.voc_keywords_output`  , UNNEST(split( keywords , ', ')) as single_words
            WHERE case_study_id = {case_study_id}
        ),
        single AS (
            SELECT DISTINCT * EXCEPT(keywords) 
            FROM tmp
        ),
        map_keyword AS (
            SELECT
                v.*,
                lower(s.single_words) as single_terms
            FROM voc_summary_table v 
            LEFT JOIN single s 
                ON s.case_study_id = v.case_study_id
                AND s.review_id = v.review_id
        )
        SELECT             
            case_study_id,
            max(case_study_name) case_study_name,
            max(dimension_config_name) dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            max(source_name) source_name,
            max(company_name) company_name,
            company_id, 
            source_id,
            polarity,
            single_terms,
            review_date as daily_date,
            count(distinct review_id) as records,
            count(distinct review_id) as collected_review_count,
            run_id
        FROM map_keyword
        GROUP BY 
            case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id, 
            source_id,
            polarity,
            single_terms,
            daily_date,
            run_id
        ;
        """
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voc_6_7_1: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voc_6_5(bqclient, case_study_id, run_id, table_6_5_competitor_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{table_6_5_competitor_table}` 

        WITH voc_summary_table AS (
            SELECT *
            FROM `{prefix}datamart_cs.summary_table_{case_study_id}`
        ),
        company_list AS (
            SELECT DISTINCT 
            case_study_id,
            company_name 
            FROM voc_summary_table
        ),
        single_alias AS (
            SELECT * 
            FROM `{prefix}dwh.company_aliases_list` , UNNEST(split(aliases, '\t')) as alias
            WHERE case_study_id = {case_study_id}  AND run_id = '{run_id}'
        ),
        alias_list AS (
            SELECT 
            case_study_id,
            company_name, 
            alias 
            FROM single_alias
        ),
        competitor_list AS (
            SELECT
            c.case_study_id,
            c.company_name,
            a.alias
            FROM company_list c
            LEFT JOIN alias_list a
            ON c.case_study_id= a.case_study_id
            AND c.company_name=a.company_name
        ),
        cte_competitor AS (
            SELECT 
            s.*,
            c.company_name as competitor,
            c.alias
            FROM  voc_summary_table as s
            LEFT JOIN competitor_list as c
            ON s.case_study_id=c.case_study_id
            WHERE s.case_study_id = {case_study_id} AND s.run_id = '{run_id}'
        ),
        cte_review AS (
            SELECT DISTINCT
            case_study_id, 
            case_study_name,
            dimension_config_name,
            dimension_config_id, 
            company_name, 
            source_name,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            dimension,
            review_date, 
            review_id, 
            trans_review AS review, 
            competitor ,
            alias,
            run_id
            FROM cte_competitor
        ),
        review_mentioned AS (
            SELECT 
                *,
                CASE
                    -- Old case: disregard alias, only check competitor in review 
                    WHEN REGEXP_CONTAINS(lower(review), CONCAT(r'\\b', lower(competitor), r'\\b')) 
                        AND lower(competitor) != lower(company_name) THEN review_id
                    -- New case: check alias in review if alias is not an empty string
                    WHEN alias != '' AND REGEXP_CONTAINS(lower(review), CONCAT(r'\\b', lower(alias), r'\\b')) 
                        AND lower(competitor) != lower(company_name) THEN review_id
                    ELSE NULL 
                END AS mentioned
            FROM cte_review
        ),
        total_mention AS ( 
            SELECT 
            case_study_id, 
            company_id,
            source_id,
            review_date,
            count(distinct case when dimension is not null then mentioned else null end) as total_review_mention,
            run_id
            FROM review_mentioned
            GROUP BY 
            case_study_id, 
            company_id,
            source_id,
            review_date,
            run_id
        )
        SELECT
            case_study_id, 
            max(case_study_name) case_study_name,
            max(dimension_config_name) dimension_config_name,
            dimension_config_id, 
            max(source_name) source_name,
            max(company_name) company_name,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            review_date as daily_date,
            competitor,
            count(distinct review_id) as records,
            count(distinct case when dimension is not null  then review_id else null end) as processed_review_count,
            (SELECT total_review_mention FROM total_mention 
            WHERE case_study_id=a.case_study_id
            AND company_id=a.company_id
            AND source_id=a.source_id
            AND review_date=a.review_date) as processed_review_mention,
            COUNT(distinct case when dimension is not null then mentioned else null end) as sum_mentioned,
            run_id
        FROM review_mentioned as a
        WHERE dimension_config_name is not null
        GROUP BY
            case_study_id,
            dimension_config_id,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            review_date,
            competitor,
            run_id
        ;
        """.format(
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            table_6_5_competitor_table=table_6_5_competitor_table,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voc_6_5: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voc_5(bqclient, case_study_id, run_id, table_5_heatmapdim_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{table_5_heatmapdim_table}` 

        WITH voc_summary_table AS (
            SELECT *
            FROM `{prefix}datamart_cs.summary_table_{case_study_id}`
        ),
        tmp AS (
            SELECT
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_name,
                company_name,
                company_id,
                source_id,
                dimension_type,
                review_id,
                review_date,
                modified_dimension,
                modified_label,
                split(terms, '\t') AS single_terms,
                polarity,
                modified_polarity,
                run_id
            FROM
                voc_summary_table
            WHERE
                dimension is not null
                AND is_used = true
        ),
        single AS (
            SELECT
                *
            EXCEPT
                (single_terms),
                single_terms
            FROM
                tmp,
                UNNEST(single_terms) AS single_terms
        ),
        count_terms AS (
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                max(source_name) source_name,
                max(company_name) company_name,
                company_id,
                source_id,
                dimension_type,
                review_date as daily_date,
                modified_dimension as dimension,
                modified_label as label,
                lower(single_terms) as single_terms,
                polarity,
                count(distinct review_id) as collected_review_count,
                a.run_id
            FROM
                single a
            WHERE
                a.dimension_config_id is not null
            GROUP BY
                a.case_study_id,
                a.dimension_config_id,
                a.nlp_type,
                a.nlp_pack,
                a.company_id,
                a.source_id,
                dimension_type,
                a.modified_dimension,
                a.modified_label,
                lower(single_terms) ,
                polarity,
                daily_date,
                a.run_id
        ),
        count_topic AS (
            SELECT
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                modified_dimension as dimension,
                modified_label as label,
                polarity,
                count(distinct review_id) topic_review_counts,
                review_date as daily_date,
                run_id
            FROM
                voc_summary_table
            WHERE
                dimension is not null 
                AND is_used = true
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                modified_dimension,
                modified_label,
                polarity,
                daily_date,
                run_id
        ),
        count_dimension AS (
            SELECT
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                modified_dimension as dimension,
                sum(
                    CASE
                        WHEN modified_polarity is null THEN null
                        ELSE modified_polarity
                    END
                ) as sum_ss,
                count(
                    CASE
                        WHEN (polarity != 'NONE')
                        AND (polarity is not null) THEN review_id
                        ELSE null
                    END
                ) as sum_review_counts,
                review_date as daily_date,
                run_id
            FROM
                voc_summary_table
            WHERE
                dimension is not null 
                AND is_used = true
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                modified_dimension,
                daily_date,
                run_id
        ),
        count_records AS (
            SELECT
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                count(distinct review_id) as records,
                review_date as daily_date,
                run_id
            FROM
                voc_summary_table
            WHERE
                dimension is not null   
                AND is_used = true
            
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                source_id,
                dimension_type,
                daily_date,
                run_id
        )
        SELECT 
            a.case_study_id,
            a.case_study_name,
            a.dimension_config_name,
            a.dimension_config_id,
            a.nlp_type,
            a.nlp_pack,
            a.source_name,
            a.company_name,
            a.company_id,
            a.source_id,
            a.daily_date,
            a.dimension,
            a.label,
            a.single_terms,
            a.polarity,
            a.collected_review_count,
            a.run_id,
            topic_review_counts,
            sum_ss,
            sum_review_counts,
            records,
            a.dimension_type
        FROM
            count_terms a
            LEFT JOIN count_topic b on a.case_study_id = b.case_study_id
            and a.dimension_config_id = b.dimension_config_id
            and a.nlp_type = b.nlp_type
            and a.nlp_pack = b.nlp_pack
            and a.company_id = b.company_id
            and a.source_id = b.source_id
            and a.dimension = b.dimension
            and a.dimension_type = b.dimension_type
            and a.label = b.label
            and a.polarity = b.polarity
            and date(a.daily_date) = date(b.daily_date)
            and a.run_id = b.run_id
            LEFT JOIN count_dimension c on a.case_study_id = c.case_study_id
            and a.dimension_config_id = c.dimension_config_id
            and a.nlp_type = c.nlp_type
            and a.nlp_pack = c.nlp_pack
            and a.company_id = c.company_id
            and a.source_id = c.source_id
            and a.dimension = c.dimension
            and a.dimension_type = c.dimension_type
            and date(a.daily_date) = date(c.daily_date)
            and a.run_id = c.run_id
            LEFT JOIN count_records d on a.case_study_id = d.case_study_id
            and a.dimension_config_id = d.dimension_config_id
            and a.nlp_type = d.nlp_type
            and a.nlp_pack = d.nlp_pack
            and a.company_id = d.company_id
            and a.source_id = d.source_id
            and a.dimension_type = d.dimension_type
            and date(a.daily_date) = date(d.daily_date)
            and a.run_id = d.run_id

            WHERE a.case_study_id = {case_study_id} and a.run_id = '{run_id}'
            ;
        """.format(
            prefix=prefix,
            run_id=run_id,
            case_study_id=case_study_id,
            table_5_heatmapdim_table=table_5_heatmapdim_table,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voc_5: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voc_3(
    bqclient,
    case_study_id,
    run_id,
    table_3_ratingtime_table,
    table_batch_status,
    table_casestudy_batchid,
    table_parent_review_mapping,
):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{table_3_ratingtime_table}` 

        WITH voc_summary_table AS (
            SELECT *
            FROM `{prefix}datamart_cs.summary_table_{case_study_id}`
            ),
        date_case_study as (
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                dimension_config_id,
                max(dimension_config_name) dimension_config_name,
                nlp_type,
                nlp_pack,
                max(review_date) as max_date,
                min(review_date) as min_date,
                GENERATE_DATE_ARRAY(min(review_date), max(review_date)) as day,
                run_id
                
            FROM
                voc_summary_table
            WHERE
                dimension_config_name is not null 
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                run_id
                
        ),
        date_company as (
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                max(source_name) source_name,
                company_id,
                max(company_name) company_name,
                (
                    SELECT
                        max(max_date)
                    FROM
                        date_case_study
                    WHERE
                        case_study_id = a.case_study_id
                        AND dimension_config_id = a.dimension_config_id
                        AND nlp_pack = a.nlp_pack
                        AND nlp_type = a.nlp_type
                        AND run_id = a.run_id
                ) as max_date,
                (
                    SELECT
                        min(min_date)
                    FROM
                        date_case_study
                    WHERE
                        case_study_id = a.case_study_id
                        AND dimension_config_id = a.dimension_config_id
                        AND nlp_pack = a.nlp_pack
                        AND nlp_type = a.nlp_type
                        AND run_id = a.run_id
                ) as min_date,
                run_id
                
            FROM
                voc_summary_table a
            WHERE
                dimension_config_name is not null
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                company_id,
                run_id
                
        ),
        date_info as (
            SELECT
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                min_date,
                max_date,
                GENERATE_DATE_ARRAY(min_date, max_date) as day,
                run_id
            FROM
                date_company
        ),
        date_range as (
            SELECT
                *
            EXCEPT
        (day),
                day
            FROM
                date_info,
                UNNEST(day) AS day
        ),
        BATCH_LIST as (
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
        parent_review AS (
        SELECT 
            DISTINCT 
            case_study_id,
            review_id,
            parent_review_id,
            technical_type
            FROM   `{table_parent_review_mapping}`
            WHERE
                    batch_id IN (
                        SELECT
                            batch_id FROM BATCH_LIST
                    )
                    AND case_study_id = {case_study_id}
        ),
        distinct_review AS(
            SELECT DISTINCT
                a.case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                review_date,
                p.parent_review_id,
                MAX(rating) as rating,
                run_id
            FROM
                voc_summary_table a
            LEFT JOIN parent_review p
                ON a.case_study_id = p.case_study_id
                AND a.review_id = p.review_id
            WHERE
                dimension_config_name is not null 
            GROUP BY
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                review_date,
                parent_review_id,
                run_id
        ),
        data as(
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                max(source_name) source_name,
                company_id,
                max(company_name) company_name,
                review_date,
                count(distinct parent_review_id) AS records,
                count(parent_review_id) as collected_review_count,
                sum(rating) as sum_rating,
                run_id
            FROM
                distinct_review

            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                company_id,
                review_date,
                run_id
        ),
        final as (
            SELECT
                d.case_study_id,
                d.case_study_name,
                d.dimension_config_name,
                d.dimension_config_id,
                d.nlp_type,
                d.nlp_pack,
                d.source_id,
                d.source_name,
                d.company_id,
                d.company_name,
                d.day as daily_date,
                dt.records,
                dt.collected_review_count,
                dt.sum_rating,
                d.run_id
            FROM
                date_range as d
                LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
                AND d.dimension_config_id = dt.dimension_config_id
                AND d.nlp_pack = dt.nlp_pack
                AND d.nlp_type = dt.nlp_type
                AND d.company_id = dt.company_id
                AND d.source_id = dt.source_id
                AND d.day = dt.review_date
                AND d.run_id = dt.run_id
                WHERE d.case_study_id = {case_study_id}
                AND d.run_id = '{run_id}'
        )
        SELECT
            case_study_id,
            case_study_name,
            dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            source_id,
            source_name,
            company_name,
            company_id,
            daily_date,
            records,
            collected_review_count AS records_daily,
            sum_rating as rating_daily,
            run_id,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                ) < 7 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA7,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                ) < 7 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA7,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                ) < 14 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA14,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                ) < 14 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA14,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                ) < 30 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA30,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                ) < 30 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA30,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                ) < 60 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA60,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                ) < 60 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA60,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                ) < 90 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA90,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                ) < 90 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA90,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                ) < 120 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA120,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                ) < 120 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA120,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                ) < 150 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA150,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                ) < 150 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA150,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                ) < 180 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA180,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                ) < 180 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA180
        FROM
            final;

        """.format(
            gcp_project_id=config.GCP_PROJECT_ID,
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            table_3_ratingtime_table=table_3_ratingtime_table,
            table_batch_status=table_batch_status,
            table_casestudy_batchid=table_casestudy_batchid,
            table_parent_review_mapping=table_parent_review_mapping,
        )
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voc_3: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


# Moving data to VoE chart's table
def insert_voe_5(bqclient, case_study_id, run_id, voe_table_5_heatmapdim_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
            INSERT INTO `{prefix}datamart.{voe_table_5_heatmapdim_table}` 
            WITH voe_summary_table AS (
                SELECT *
                FROM `{prefix}datamart_cs.voe_summary_table_{case_study_id}`
            ),
            tmp AS (
                SELECT
                    case_study_id,
                    case_study_name,
                    dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    source_name,
                    company_name,
                    company_id,
                    source_id,
                    review_id,
                    review_date,
                    modified_dimension,
                    modified_label,
                    split(terms, '\t') AS single_terms,
                    polarity,
                    modified_polarity,
                    run_id
                FROM
                    voe_summary_table
                WHERE
                    dimension is not null
                    AND is_used = true
                    AND case_study_id = {case_study_id} 
                    AND run_id = '{run_id}'
            ),
            single AS (
                SELECT
                    * EXCEPT (single_terms),
                    single_terms
                FROM
                    tmp,
                    UNNEST(single_terms) AS single_terms
            ),
            count_terms AS (
                SELECT
                    case_study_id,
                    max(case_study_name) case_study_name,
                    max(dimension_config_name) dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    max(source_name) source_name,
                    max(company_name) company_name,
                    company_id,
                    source_id,
                    review_date as daily_date,
                    modified_dimension as dimension,
                    modified_label as label,
                    lower(single_terms) as single_terms,
                    polarity,
                    count(distinct review_id) as collected_review_count,
                    a.run_id
                FROM
                    single a
                WHERE
                    a.dimension_config_id is not null
                GROUP BY
                    a.case_study_id,
                    a.dimension_config_id,
                    a.nlp_type,
                    a.nlp_pack,
                    a.company_id,
                    a.source_id,
                    a.modified_dimension,
                    a.modified_label,
                    lower(single_terms) ,
                    polarity,
                    daily_date,
                    a.run_id
            ),
            count_topic AS (
                SELECT
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    modified_dimension as dimension,
                    modified_label as label,
                    polarity,
                    count(distinct review_id) topic_review_counts,
                    review_date as daily_date,
                    run_id
                FROM
                    voe_summary_table
                WHERE
                    dimension is not null 
                    AND is_used = true
                    AND case_study_id = {case_study_id}
                    AND run_id = '{run_id}'
                GROUP BY
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    modified_dimension,
                    modified_label,
                    polarity,
                    daily_date,
                    run_id
            ),
            count_dimension AS (
                SELECT
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    modified_dimension as dimension,
                    sum(
                        CASE
                            WHEN modified_polarity is null THEN null
                            ELSE modified_polarity
                        END
                    ) as sum_ss,
                    count(
                        CASE
                            WHEN (polarity != 'NONE')
                            AND (polarity is not null) THEN review_id
                            ELSE null
                        END
                    ) as sum_review_counts,
                    review_date as daily_date,
                    run_id
                FROM
                    voe_summary_table
                WHERE
                    dimension is not null 
                    AND is_used = true
                    AND case_study_id = {case_study_id}
                    AND run_id = '{run_id}'
                GROUP BY
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    modified_dimension,
                    daily_date,
                    run_id
            ),
            count_records AS (
                SELECT
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    count(distinct review_id) as records,
                    review_date as daily_date,
                    run_id
                FROM
                    voe_summary_table
                WHERE
                    dimension is not null   
                    AND is_used = true
                    AND case_study_id = {case_study_id} 
                    AND run_id = '{run_id}'
                GROUP BY
                    case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id,
                    daily_date,
                    run_id
            )
            SELECT
                a.*,
                topic_review_counts,
                sum_ss,
                sum_review_counts,
                records
            FROM
                count_terms a
            LEFT JOIN count_topic b ON a.case_study_id = b.case_study_id
                AND a.dimension_config_id = b.dimension_config_id
                AND a.nlp_type = b.nlp_type
                AND a.nlp_pack = b.nlp_pack
                AND a.company_id = b.company_id
                AND a.source_id = b.source_id
                AND a.dimension = b.dimension
                AND a.label = b.label
                AND a.polarity = b.polarity
                AND date(a.daily_date) = date(b.daily_date)
                AND a.run_id = b.run_id
            LEFT JOIN count_dimension c ON a.case_study_id = c.case_study_id
                AND a.dimension_config_id = c.dimension_config_id
                AND a.nlp_type = c.nlp_type
                AND a.nlp_pack = c.nlp_pack
                AND a.company_id = c.company_id
                AND a.source_id = c.source_id
                AND a.dimension = c.dimension
                AND date(a.daily_date) = date(c.daily_date)
                AND a.run_id = c.run_id
            LEFT JOIN count_records d ON a.case_study_id = d.case_study_id
                AND a.dimension_config_id = d.dimension_config_id
                AND a.nlp_type = d.nlp_type
                AND a.nlp_pack = d.nlp_pack
                AND a.company_id = d.company_id
                AND a.source_id = d.source_id
                AND date(a.daily_date) = date(d.daily_date)
                AND a.run_id = d.run_id
            ;
        """.format(
            prefix=prefix,
            run_id=run_id,
            case_study_id=case_study_id,
            voe_table_5_heatmapdim_table=voe_table_5_heatmapdim_table,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voe_5: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voe_11(bqclient, case_study_id, run_id, voe_11_termcount_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{voe_11_termcount_table}` 

        WITH tmp AS (
            SELECT 
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_name,
                company_name,
                company_id, 
                source_id,
                review_id,
                review_date,
                modified_dimension ,
                modified_label,
                split(terms,'\t') AS single_terms,
                polarity,
                modified_polarity,
                run_id
            FROM `{prefix}datamart_cs.voe_summary_table_*`
            WHERE _TABLE_SUFFIX = '{case_study_id}'
                and dimension is not null    
                and dimension_config_name is not null
                and is_used = true
                and case_study_id = {case_study_id}
        ),
        single AS (
            SELECT 
                *
                EXCEPT(single_terms), 
                single_terms 
            FROM 
                tmp, 
                UNNEST(single_terms) AS single_terms
        )
        SELECT             
            case_study_id,
            max(case_study_name) case_study_name,
            max(dimension_config_name) dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            max(source_name) source_name,
            max(company_name) company_name,
            company_id, 
            source_id,
            polarity,
            lower(single_terms) as single_terms,
            review_date as daily_date,
            count(distinct review_id) as records,
            count(distinct review_id) as collected_review_count,
            run_id
        FROM single
        GROUP BY 
            case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id, 
            source_id,
            polarity,
            lower(single_terms),
            daily_date,
            run_id
        ;
		""".format(
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            voe_11_termcount_table=voe_11_termcount_table,
        )
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voe_11: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voe_11_1(bqclient, case_study_id, run_id, VOE_11_1_keywordcount_table_id):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = f"""
        INSERT INTO `{prefix}datamart.{VOE_11_1_keywordcount_table_id}`  

        WITH voe_summary_table AS (
            SELECT DISTINCT
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_name,
                company_name,
                company_id, 
                source_id,
                polarity,
                review_date,
                review_id,
                run_id
            FROM `{prefix}datamart_cs.voe_summary_table_*`
            WHERE _TABLE_SUFFIX = '{case_study_id}'
                AND run_id = '{run_id}'
                AND dimension IS NOT NULL 
                AND dimension_config_name IS NOT NULL 
                AND is_used = true
        ),
        tmp AS (SELECT *
            FROM `{prefix}staging.voe_keywords_output`  , UNNEST(split( keywords , ', ')) as single_words
            WHERE case_study_id = {case_study_id}
        ),
        single AS (
            SELECT DISTINCT * EXCEPT(keywords) 
            FROM tmp
        ),
        map_keyword AS (
            SELECT
                v.*,
                lower(s.single_words) as single_terms
            FROM voe_summary_table v 
            LEFT JOIN single s 
                ON s.case_study_id = v.case_study_id
                AND s.review_id = v.review_id
        )
        SELECT             
            case_study_id,
            max(case_study_name) case_study_name,
            max(dimension_config_name) dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            max(source_name) source_name,
            max(company_name) company_name,
            company_id, 
            source_id,
            polarity,
            single_terms,
            review_date as daily_date,
            count(distinct review_id) as records,
            count(distinct review_id) as collected_review_count,
            run_id
        FROM map_keyword
        GROUP BY 
            case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id, 
            source_id,
            polarity,
            single_terms,
            daily_date,
            run_id
        ;
		"""
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voe_11_1: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voe_3(
    bqclient,
    case_study_id,
    run_id,
    voe_table_3_ratingtime_table,
    table_batch_status,
    table_casestudy_batchid,
    table_parent_review_mapping,
):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{voe_table_3_ratingtime_table}` 

        WITH voe_summary_table AS (
            SELECT *
            FROM `{prefix}datamart_cs.voe_summary_table_{case_study_id}`
        ),
        date_case_study as (
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                dimension_config_id,
                max(dimension_config_name) dimension_config_name,
                nlp_type,
                nlp_pack,
                max(review_date) as max_date,
                min(review_date) as min_date,
                GENERATE_DATE_ARRAY(min(review_date), max(review_date)) as day,
                run_id
                
            FROM
                voe_summary_table
            WHERE
                dimension_config_name is not null 
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                run_id
                
        ),
        date_company as (
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                max(source_name) source_name,
                company_id,
                max(company_name) company_name,
                (
                    SELECT
                        max(max_date)
                    FROM
                        date_case_study
                    WHERE
                        case_study_id = a.case_study_id
                        AND dimension_config_id = a.dimension_config_id
                        AND nlp_pack = a.nlp_pack
                        AND nlp_type = a.nlp_type
                        AND run_id = a.run_id
                ) as max_date,
                (
                    SELECT
                        min(min_date)
                    FROM
                        date_case_study
                    WHERE
                        case_study_id = a.case_study_id
                        AND dimension_config_id = a.dimension_config_id
                        AND nlp_pack = a.nlp_pack
                        AND nlp_type = a.nlp_type
                        AND run_id = a.run_id
                ) as min_date,
                run_id
                
            FROM
                voe_summary_table a
            WHERE
                dimension_config_name is not null
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                company_id,
                run_id
                
        ),
        date_info as (
            SELECT
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                min_date,
                max_date,
                GENERATE_DATE_ARRAY(min_date, max_date) as day,
                run_id
            FROM
                date_company
        ),
        date_range as (
            SELECT
                *
            EXCEPT
        (day),
                day
            FROM
                date_info,
                UNNEST(day) AS day
        ),
         BATCH_LIST as (
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
        parent_review AS (
        SELECT 
            DISTINCT 
            case_study_id,
            review_id,
            parent_review_id,
            technical_type
            FROM   `{table_parent_review_mapping}`
            WHERE
                    batch_id IN (
                        SELECT
                            batch_id FROM BATCH_LIST
                    )
                    AND case_study_id = {case_study_id}
        ),
        distinct_review AS(
            SELECT DISTINCT
                a.case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                review_date,
                p.parent_review_id,
                MAX(rating) as rating,
                run_id
            FROM
                voe_summary_table a
            LEFT JOIN parent_review p
                ON a.case_study_id = p.case_study_id
                AND a.review_id = p.review_id
            WHERE
                dimension_config_name is not null 
            GROUP BY
                case_study_id,
                case_study_name,
                dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                source_name,
                company_id,
                company_name,
                review_date,
                parent_review_id,
                run_id
        ),
        data as(
            SELECT
                case_study_id,
                max(case_study_name) case_study_name,
                max(dimension_config_name) dimension_config_name,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                max(source_name) source_name,
                company_id,
                max(company_name) company_name,
                review_date,
                count(distinct parent_review_id) AS records,
                count(parent_review_id) as collected_review_count,
                sum(rating) as sum_rating,
                run_id
            FROM
                distinct_review
            GROUP BY
                case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                source_id,
                company_id,
                review_date,
                run_id
        ),
        final as (
            SELECT
                d.case_study_id,
                d.case_study_name,
                d.dimension_config_name,
                d.dimension_config_id,
                d.nlp_type,
                d.nlp_pack,
                d.source_id,
                d.source_name,
                d.company_id,
                d.company_name,
                d.day as daily_date,
                dt.records,
                dt.collected_review_count,
                dt.sum_rating,
                d.run_id
            FROM
                date_range as d
                LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
                AND d.dimension_config_id = dt.dimension_config_id
                AND d.nlp_pack = dt.nlp_pack
                AND d.nlp_type = dt.nlp_type
                AND d.company_id = dt.company_id
                AND d.source_id = dt.source_id
                AND d.day = dt.review_date
                AND d.run_id = dt.run_id
                WHERE d.case_study_id = {case_study_id}
                AND d.run_id = '{run_id}'
        )
        SELECT
            case_study_id,
            case_study_name,
            dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            source_id,
            source_name,
            company_name,
            company_id,
            daily_date,
            records,
            collected_review_count AS records_daily,
            sum_rating as rating_daily,
            run_id,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                ) < 7 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA7,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                ) < 7 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 6 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA7,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                ) < 14 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA14,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                ) < 14 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 13 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA14,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                ) < 30 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA30,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                ) < 30 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 29 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA30,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                ) < 60 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA60,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                ) < 60 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 59 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA60,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                ) < 90 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA90,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                ) < 90 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 89 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA90,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                ) < 120 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA120,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                ) < 120 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 119 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA120,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                ) < 150 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA150,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                ) < 150 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 149 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA150,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                ) < 180 THEN NULL
                ELSE sum(sum_rating) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                )
            END AS RATING_MA180,
            CASE
                WHEN COUNT(daily_date) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                ) < 180 THEN NULL
                ELSE sum(collected_review_count) OVER (
                    PARTITION BY case_study_id,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    company_id,
                    source_id
                    ORDER BY
                        daily_date ROWS BETWEEN 179 PRECEDING
                        AND CURRENT ROW
                )
            END AS RECORDS_MA180
        FROM
            final;
        """.format(
            gcp_project_id=config.GCP_PROJECT_ID,
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            voe_table_3_ratingtime_table=voe_table_3_ratingtime_table,
            table_batch_status=table_batch_status,
            table_casestudy_batchid=table_casestudy_batchid,
            table_parent_review_mapping=table_parent_review_mapping,
        )
        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voe_3: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def insert_voe_6_5(bqclient, case_study_id, run_id, voe_table_6_5_competitor_table):
    try:
        prefix = f"{config.GCP_PROJECT_ID}.{config.PREFIX}_"
        if config.PREFIX == "dev":
            prefix = f"{config.GCP_PROJECT_ID}."

        sql = """
        INSERT INTO `{prefix}datamart.{voe_table_6_5_competitor_table}` 

        WITH voe_summary_table AS (
            SELECT *
            FROM `{prefix}datamart_cs.voe_summary_table_{case_study_id}`
        ),
        company_list AS (
            SELECT DISTINCT 
            case_study_id,
            company_name 
            FROM voe_summary_table
        ),
        single_alias AS (
            SELECT * 
            FROM `{prefix}dwh.voe_company_aliases_list` , UNNEST(split(aliases, '\t')) as alias
            WHERE case_study_id = {case_study_id}  AND run_id = '{run_id}'
        ),
        alias_list AS (
            SELECT 
            case_study_id,
            company_name, 
            alias 
            FROM single_alias
        ),
        competitor_list AS (
            SELECT
            c.case_study_id,
            c.company_name,
            a.alias
            FROM company_list c
            LEFT JOIN alias_list a
            ON c.case_study_id= a.case_study_id
            AND c.company_name=a.company_name
        ),
        cte_competitor AS (
            SELECT 
            s.*,
            c.company_name as competitor,
            c.alias
            FROM  voe_summary_table as s
            LEFT JOIN competitor_list as c
            ON s.case_study_id=c.case_study_id
            WHERE s.case_study_id = {case_study_id} AND s.run_id = '{run_id}'
        ),
        cte_review AS (
            SELECT DISTINCT
            case_study_id, 
            case_study_name,
            dimension_config_name,
            dimension_config_id, 
            company_name, 
            source_name,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            dimension,
            review_date, 
            review_id, 
            trans_review AS review, 
            competitor ,
            alias,
            run_id
            FROM cte_competitor
        ),
        review_mentioned AS (
            SELECT 
                *,
                CASE
                    -- Old case: disregard alias, only check competitor in review
                    WHEN REGEXP_CONTAINS(lower(review), CONCAT(r'\\b', lower(competitor), r'\\b')) 
                        AND lower(competitor) != lower(company_name) THEN review_id
                    -- New case: check alias in review if alias is not an empty string
                    WHEN alias != '' AND REGEXP_CONTAINS(lower(review), CONCAT(r'\\b', lower(alias), r'\\b')) 
                        AND lower(competitor) != lower(company_name) THEN review_id
                    ELSE NULL 
                END AS mentioned
            FROM cte_review
        ),
        total_mention AS ( 
            SELECT 
            case_study_id, 
            company_id,
            source_id,
            review_date,
            count(distinct case when dimension is not null then mentioned else null end) as total_review_mention,
            run_id
            FROM review_mentioned
            GROUP BY 
            case_study_id, 
            company_id,
            source_id,
            review_date,
            run_id
        )
        SELECT
            case_study_id, 
            max(case_study_name) case_study_name,
            max(dimension_config_name) dimension_config_name,
            dimension_config_id, 
            max(source_name) source_name,
            max(company_name) company_name,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            review_date as daily_date,
            competitor,
            count(distinct review_id) as records,
            count(distinct case when dimension is not null  then review_id else null end) as processed_review_count,
            (SELECT total_review_mention FROM total_mention 
            WHERE case_study_id=a.case_study_id
            AND company_id=a.company_id
            AND source_id=a.source_id
            AND review_date=a.review_date) as processed_review_mention,
            COUNT(distinct case when dimension is not null then mentioned else null end) as sum_mentioned,
            run_id
        FROM review_mentioned as a
        WHERE dimension_config_name is not null
        GROUP BY
            case_study_id,
            dimension_config_id,
            company_id, 
            source_id,
            nlp_type,
            nlp_pack,
            review_date,
            competitor,
            run_id
        ;
        """.format(
            run_id=run_id,
            prefix=prefix,
            case_study_id=case_study_id,
            voe_table_6_5_competitor_table=voe_table_6_5_competitor_table,
        )

        query_job = bqclient.query(sql)
        query_job.result()
        return True
    except Exception as error:
        logger.exception(
            f"[MOVING_DATA] __insert_voe_6_5: case_study_id={case_study_id}, error={error.__class__.__name__}"
        )
        return False


def route_handle_functions(payload):
    nlp_type: str = payload["payload"]["dimension_config"]["nlp_type"].lower()
    if nlp_type in ("voc", "voe"):
        return handle_task_voc_voe(payload)
    elif nlp_type == "hra":
        return hra_coresignal.handle_task_hra(payload)
    else:
        mess = f"Invalid nlp_type: {nlp_type}"
        logger.error(mess)
        raise Exception(mess)


if __name__ == "__main__":
    pubsub_util.subscribe(
        logger, config.GCP_PROJECT_ID, config.GCP_PUBSUB_SUBSCRIPTION, route_handle_functions
    )
