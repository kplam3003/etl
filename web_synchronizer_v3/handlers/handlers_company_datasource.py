import sys
sys.path.append('../')
import config
from postgres import postgres_company_datasource, postgres_common
from logger_global import logger
from helpers import CompanyExistsAndRunningError, validate_payload_fields
from core import pubsub_util


def handle(
    payload,
):
    """
    Write metadata of company_datasource request into MDM database.
    """
    mandatory_fields = config.COMPANY_DATASOURCE_PAYLOAD_MANDATORY_FIELDS
    logger.info("[handle_postgres_company_datasource] starts")
    data_type = payload.get("data_type")
    try:
        # I. Verify payload
        is_valid_payload = validate_payload_fields(
            payload=payload,
            mandatory_fields=mandatory_fields,
        )
        if not is_valid_payload:
            raise Exception("Invalid payload!")
        company_datasource_id = payload['company_datasource_id']
        # check existence
        etl_company_datasource = postgres_company_datasource.get_etl_company_datasource(
            company_datasource_id=company_datasource_id,
            data_type=data_type,
        )
        # II. if etl_company_datasource exists  => update version
        if etl_company_datasource:
            etl_request, etl_company_datasource, new_batches_ids = \
                    handle_company_datasource_exists(
                    payload=payload,
                    data_type=data_type,
                    current_etl_company_datasource=etl_company_datasource
                )
        # III. etl_company_datasource does not exist => new
        else:
            etl_request, etl_company_datasource, new_batches_ids = \
                handle_company_datasource_new(payload=payload, data_type=data_type)

        logger.info("[handle_postgres_company_datasource] completes")
        return etl_request, etl_company_datasource, new_batches_ids
    except Exception as e:
        logger.exception("[handle_postgres_company_datasource] fails")
        return None, None, None


def handle_company_datasource_exists(
    payload,
    data_type,
    current_etl_company_datasource=None
):
    """
    Insert to postgres for existing company_datasource
    - Get the information of current etl_company_datasource with given id or use provided one
    - Check status: only proceed if current etl_company_datasource is finished or completed_with_error
    - Create new etl_request
    - Update company_datasource:
        - associating company_datasource with the new request_id
        - set status to waiting
        - set progress to 0.0
        - increment data_version
        - replace existing payload with new payload
    """
    request_type = config.COMPANY_DATASOURCE_REQUEST_TYPE
    etl_company_datasource = current_etl_company_datasource.copy()
    company_datasource_id = etl_company_datasource['company_datasource_id']
    logger.info(f"[handle_company_datasource_exists] start handling "
                f"company_datasource_id {company_datasource_id}")

    # 1. get the existing etl_company_datasource or use the provided one if applicable
    assert company_datasource_id == etl_company_datasource['company_datasource_id'], \
        f'ids from payload and etl_company_datasource mismatch: \
        {company_datasource_id} and {etl_company_datasource["company_datasource_id"]}'

    # 2. check status
    if etl_company_datasource['status'] in ('running', 'waiting'):
        raise CompanyExistsAndRunningError(f"[handle_postgres_company_datasource_exists] "
                        f"company_datasource {company_datasource_id} exists "
                        f"and is {etl_company_datasource['status']}! "
                        "Request refused!")

    # 3. if current status is valid, then create new etl_request
    new_etl_request = postgres_common.create_etl_request(
        request_type=request_type,
        data_type=data_type,
        payload=payload
    )
    # 4. update status and data_version
    updated_etl_company_datasource = postgres_company_datasource.update_existing_company_datasource(
        company_datasource_id=company_datasource_id,
        request_id=new_etl_request['request_id'],
        data_type=data_type,
        payload=payload,
    )
    # 5. create new batches to run
    if payload['translation_service']:
        do_translate = True
    else:
        do_translate = False

    url_dict = updated_etl_company_datasource['urls']
    new_batches_ids = []
    for ud in url_dict:
        # NOTE: Because data_type previously can only be `job` or `review`,
        # and in case pf `job`, there are 2 implicit data_type: `overview` and `job`
        # so we checked the type of url and take that to be batch_type (decompose)
        # Now, review_stats use the same URL with review, and BE still send url type as
        # `review`, so we need to have a check here to set batch_type to `review_stats`
        if data_type in [config.JOB_DATA_TYPE, config.REVIEW_DATA_TYPE]:
            batch_type = ud["type"]
        elif data_type in [config.REVIEW_STATS_DATA_TYPE, config.CORESIGNAL_STATS_DATA_TYPE, config.CORESIGNAL_EMPLOYEES_DATA_TYPE]:
            batch_type = data_type

        url = ud["url"]
        new_etl_company_batch = postgres_common.create_etl_company_batch(
            request_id=new_etl_request['request_id'],
            company_datasource_id=updated_etl_company_datasource['company_datasource_id'],
            company_id=updated_etl_company_datasource['company_id'],
            company_name=updated_etl_company_datasource['company_name'],
            source_id=updated_etl_company_datasource['source_id'],
            source_name=updated_etl_company_datasource['source_name'],
            source_type=updated_etl_company_datasource['source_type'],
            source_code=updated_etl_company_datasource['source_code'],
            batch_type=batch_type,
            url=url,
            nlp_type=updated_etl_company_datasource['nlp_type'],
            is_translation=do_translate,
            data_version=updated_etl_company_datasource['data_version'],
        )
        new_batches_ids.append(new_etl_company_batch['batch_id'])

    logger.info(f"[handle_postgres_company_datasource_exists] "
                f"company_datasource_id {company_datasource_id} "
                "handled successfully")
    return new_etl_request, updated_etl_company_datasource, new_batches_ids

def handle_company_datasource_new(payload, data_type):
    """
    Insert to postgres for new company_datasource:
    - create new etl_request
    - create new etl_company_datasource
    - create new batches for data_transform
    """
    # here we handle when a company_datasource_id does not exists
    request_type = config.COMPANY_DATASOURCE_REQUEST_TYPE
    company_datasource_id = payload['company_datasource_id']

    logger.info(f"[handle_postgres_company_datasource_new] start handling "
                f"company_datasource_id {company_datasource_id}")
    # 1. create new etl_request entry
    new_etl_request =  postgres_common.create_etl_request(
        request_type=request_type,
        payload=payload,
        data_type=data_type,
    )
    # 2. create new etl_company_datasource entry
    new_etl_company_datasource = postgres_company_datasource.create_etl_company_datasource(
        company_datasource_id=company_datasource_id,
        request_id=new_etl_request['request_id'],
        company_id=payload['company_id'],
        company_name=payload['company_name'],
        source_id=payload['source_id'],
        source_name=payload['source_name'],
        source_code=payload['source_code'],
        source_type=payload['source_type'],
        nlp_type=payload['nlp_type'],
        urls=payload['urls'],
        data_type=data_type,
        payload=payload,
    )
    # 3. create batches to run
    if payload['translation_service']:
        do_translate = True
    else:
        do_translate = False

    url_dict = new_etl_company_datasource['urls']
    new_batches_ids = []
    for ud in url_dict:
        # NOTE: Because data_type previously can only be `job` or `review`,
        # and in case pf `job`, there are 2 implicit data_type: `overview` and `job`
        # so we checked the type of url and take that to be batch_type (decompose)
        # Now, review_stats use the same URL with review, and BE still send url type as
        # `review`, so we need to have a check here to set batch_type to `review_stats`
        if data_type in [config.JOB_DATA_TYPE, config.REVIEW_DATA_TYPE]:
            batch_type = ud["type"]
        elif data_type in [config.REVIEW_STATS_DATA_TYPE, config.CORESIGNAL_STATS_DATA_TYPE, config.CORESIGNAL_EMPLOYEES_DATA_TYPE]:
            batch_type = data_type
        else:
            raise Exception(f"Unsupported data_type: {data_type}")

        url = ud["url"]
        new_etl_company_batch = postgres_common.create_etl_company_batch(
            request_id=new_etl_request['request_id'],
            company_datasource_id=new_etl_company_datasource['company_datasource_id'],
            company_id=new_etl_company_datasource['company_id'],
            company_name=new_etl_company_datasource['company_name'],
            source_id=new_etl_company_datasource['source_id'],
            source_name=new_etl_company_datasource['source_name'],
            source_type=new_etl_company_datasource['source_type'],
            source_code=new_etl_company_datasource['source_code'],
            batch_type=batch_type,
            url=url,
            nlp_type=new_etl_company_datasource['nlp_type'],
            is_translation=do_translate,
            data_version=new_etl_company_datasource['data_version'],
        )
        new_batches_ids.append(new_etl_company_batch['batch_id'])

    logger.info(f"[handle_postgres_company_datasource_new] "
                f"company_datasource_id {company_datasource_id} "
                "handled successfully")
    return new_etl_request, new_etl_company_datasource, new_batches_ids


def publish_pubsub_company_datasource(request_id, data_type, company_datasource_id):
    # send progress -1.0 to progress topic to acknowledge request
    progress_payload = {
        'event': config.PROGRESS_EVENT,
        'company_datasource_id': company_datasource_id,
        'data_type': data_type,
        'progress': config.PROGRESS
    }
    pubsub_util.publish(
        logger,
        config.GCP_PROJECT_ID,
        config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_PROGRESS,
        progress_payload
    )
    # send message to data_flow to starts ETL process
    data_flow_payload = {
        'request_id': request_id,
        'event': config.DATA_TRANSFORM_EVENT
    }
    pubsub_util.publish(
        logger,
        config.GCP_PROJECT_ID,
        config.GCP_PUBSUB_TOPIC_DATA_FLOW,
        data_flow_payload
    )

