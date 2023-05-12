import sys
sys.path.append('../')


import config
from handlers import handlers_company_datasource, handlers_case_study
from core import pubsub_util, logger

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def handle_task(payload):
    logger.info("-------------------------[HANDLE_TASK] Begin")
    try:
        request_type = payload['type']
        # handle company_datasource
        if request_type == config.COMPANY_DATASOURCE_REQUEST_TYPE:
            # handle postgres first
            etl_request, etl_company_datasource, new_batches_ids = handlers_company_datasource.handle(payload)
            if not etl_request:
                raise Exception('company_datasource postgres handler fails')
            # publish pubsub
            handlers_company_datasource.publish_pubsub_company_datasource(
                request_id=etl_request['request_id'],
                data_type=etl_request['data_type'],
                company_datasource_id=etl_company_datasource['company_datasource_id']
            )
        # handle case_study
        elif request_type == config.CASE_STUDY_REQUEST_TYPE:
            action = payload['action']
            # handle postgres
            etl_request, etl_case_study, recrawl_ids, recrawl_etl_company_datasources, etl_case_study_data_list = \
                handlers_case_study.handle(payload=payload)
            if not etl_request:
                raise Exception('case_study postgres handler fails')

            # publish pubsub to company_datasource_data_flow to start etl if 
            # - cs payload has re-crawl requests, 
            # - and those recrawl requests are successfully created in mdm db
            if recrawl_etl_company_datasources:
                for _dict, data_type in recrawl_etl_company_datasources:
                    handlers_company_datasource.publish_pubsub_company_datasource(
                        request_id=_dict['request_id'],
                        data_type=data_type,
                        company_datasource_id=_dict['company_datasource_id']
                    )

            # data_flow payload for data consume will always have the cs_data_version field
            # - sync: previous_data_version = 0, new_data_version = latest possible data_version when cs is created
            # - update: previous_data_version = version before being updated
            # - dimension_change: empty list
            cs_data_version_changes = []
            for etl_case_study_data in etl_case_study_data_list:
                cs_data_version_changes.append({
                    "company_datasource_id": etl_case_study_data["company_datasource_id"],
                    "previous_data_version": etl_case_study_data.get("previous_data_version", 0),
                    "new_data_version": etl_case_study_data["data_version"],
                    "data_type": etl_case_study_data["data_type"]
                })
            handlers_case_study.publish_pubsub_case_study(
                request_id=etl_request['request_id'],
                case_study_id=etl_case_study['case_study_id'],
                action=action,
                cs_data_version_changes=cs_data_version_changes,
            )

        logger.info("-------------------------[HANDLE_TASK] End: Success")
    except Exception as error:
        logger.exception(f"-------------------------[HANDLE_TASK] End: {error}")


if __name__ == "__main__":
    pubsub_util.subscribe(
        logger,
        config.GCP_PROJECT_ID,
        config.GCP_PUBSUB_SUBSCRIPTION_REQUESTS_WEBPLATFORM,
        handle_task
    )
