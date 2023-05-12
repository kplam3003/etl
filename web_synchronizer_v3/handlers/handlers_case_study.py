import json
import sys
sys.path.append('../')
import config
from postgres import postgres_case_study, postgres_common, postgres_company_datasource
from handlers import handlers_company_datasource
from core import pubsub_util
from logger_global import logger
from helpers import validate_payload_fields, CompanyExistsAndRunningError


def handle(payload):
    """
    Write metadata of case_study request into MDM database
    """
    mandatory_fields = config.CASE_STUDY_PAYLOAD_MANDATORY_FIELDS
    logger.info("[Handle casestudy] starts")
    try:
        # I. work on payload
        # verify
        is_valid_payload = validate_payload_fields(payload=payload,
                                                    mandatory_fields=mandatory_fields,
                                                    )
        if not is_valid_payload:
            raise Exception("Invalid payload!")

        case_study_id = payload['case_study_id']
        request_type = payload['type']
        action = payload['action']
        # II. handle actions

        recrawl_ids = []
        recrawl_etl_company_datasources = []
        etl_case_study_data_list = []
        if action == config.SYNC_ACTION:
            etl_request, etl_case_study, etl_case_study_data_list, recrawl_ids, recrawl_etl_company_datasources = \
                    handle_case_study_sync(payload=payload)

        elif action == config.DIMENSION_CHANGE_ACTION:
            etl_request, etl_case_study, etl_case_study_data_list = \
                handle_case_study_dimension_change(payload=payload)

        elif action == config.UPDATE_ACTION:
            etl_request, etl_case_study, etl_case_study_data_list = \
                handle_case_study_update(payload=payload)

        else:
            raise Exception(f'[handle_postgres_case_study] action {action} not supported!')

        logger.info("[handle_postgres_case_study] completes")
        return etl_request, etl_case_study, recrawl_ids, recrawl_etl_company_datasources, etl_case_study_data_list

    except:
        logger.exception("[handle_postgres_case_study] fails")
        return None, None, None, None, None


## START SYNC
def handle_case_study_sync(payload):
    """
    Insert to postgres for new case_study:
    - create new etl_request
    - create new etl_case_study
    - create new etl_case_study_data entries

    NOTE: If a company_datasource is added while having in-storage status other than finished
    and completed with error, the company_datasource will be added with version equal in-storage's
    version - 1. E.g if company_datasource_id 15 is running at version 2, and is added into a cs,
    that cs will have version 1. However, if in-storage version is 1, there will be a warning, and
    data source will still be added. Case_study orchestrator will wait for the CS to finish before
    proceed with data_consume downstream tasks
    """
    case_study_id = payload['case_study_id']
    case_study_name = payload['case_study_name']
    nlp_type = payload['company_datasources'][0]['nlp_type']
    nlp_pack = payload['company_datasources'][0]['nlp_pack']
    nlp_type = payload['dimension_config']['nlp_type']
    logger.info(f"[handle_postgres_case_study_sync] start handling "
                f"case_study_id {case_study_id}, "
                f"case_study_name {case_study_name}")

    recrawl_etl_company_datasources = []
    # I. check if case study exists
    etl_case_study = postgres_case_study.get_etl_case_study(case_study_id=case_study_id)
    if etl_case_study:
        del etl_case_study
        raise Exception(f"case_study_id {case_study_id} already exists! Request rejected.")

    # II. create new etl_request
    etl_request = postgres_common.create_etl_request(
        request_type=config.CASE_STUDY_REQUEST_TYPE,
        payload=payload,
        data_type=nlp_type,
    )
    request_id = etl_request['request_id']

    # III. create new etl_case_study
    new_etl_case_study = postgres_case_study.create_etl_case_study(
        request_id=request_id,
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        nlp_type=nlp_type,
        nlp_pack=nlp_pack,
        payload=payload,
    )

    # IV. handle recrawl requests
    # 1. extract recrawl case studies
    all_ids, recrawl_ids = extract_company_datasource_id_from_case_study_payload(payload)
    # 2. construct valid payloads for company_datasource handler IF recrawl_id has values
    if recrawl_ids:
        recrawl_payloads = construct_existing_company_datasource_payloads(recrawl_ids, nlp_type)
        # 2.1. send recrawl payloads to company_datasource postgres handler
        recrawl_ids, recrawl_payloads, recrawl_etl_company_datasources = handle_recrawl(recrawl_ids, recrawl_payloads)
    # V. create new entries in mdm.etl_case_study_data
    etl_case_study_data_list = []
    company_datasources_all = get_company_datasources(payload['company_datasources'], nlp_type)
    for _etl_company_datasource, data_type_company_datasource, payload_company_datasource in company_datasources_all:
        if not _etl_company_datasource:
            logger.warning(
                f'[handle_postgres_case_study_sync] company_datasource not found in DB! '
                f'Maybe it is not triggered at least once yet? '
                f'Data type: {data_type_company_datasource}, '
                f'Payload {json.dumps(payload_company_datasource)}'
            )
            continue

        # handle data version
        _in_storage_company_datasource_id = _etl_company_datasource['company_datasource_id']
        _in_storage_data_version = _etl_company_datasource['data_version']
        _in_storage_status = _etl_company_datasource['status']
        _in_storage_progress = _etl_company_datasource['progress']
        # running company_datasources that are not initiated by this case_study
        # will be added with previous version
        if _in_storage_status not in ('finished', 'completed with error'):
            if _in_storage_data_version == 1:
                logger.warning("[handle_postgres_dimension_change_case_study] "
                              f"company_datasource_id {_in_storage_company_datasource_id} "
                              f"has data version {_in_storage_data_version} "
                              f"and has status {_in_storage_status}")
                data_version = _in_storage_data_version
                status = _in_storage_status
                progress = _in_storage_progress

            elif _in_storage_company_datasource_id in recrawl_ids:
                data_version = _in_storage_data_version
                status = _in_storage_status
                progress = _in_storage_progress

            else:
                data_version = _in_storage_data_version - 1
                status = config.FINISHED_STATUS
                progress = 1.0

        else:
            data_version = _in_storage_data_version
            status = _in_storage_status
            progress = _in_storage_progress

        _etl_case_study_data = postgres_case_study.create_etl_case_study_data(
            case_study_id=case_study_id,
            request_id=request_id,
            company_datasource_id=payload_company_datasource['company_datasource_id'],
            nlp_type=payload_company_datasource['nlp_type'],
            nlp_pack=payload_company_datasource['nlp_pack'],
            re_scrape=payload_company_datasource['re_scrape'],
            re_nlp=payload_company_datasource['re_nlp'],
            data_version=data_version,
            data_type=data_type_company_datasource,
            status=status,
            progress=progress,
            payload=payload_company_datasource,
        )
        if _etl_case_study_data:
            etl_case_study_data_list.append(_etl_case_study_data)
        else:
            logger.warning(f'[handle_postgres_case_study_sync] failed to create '
                           f'etl_case_study_data entry for '
                           f'payload {payload_company_datasource}')

    logger.info(f"[handle_postgres_case_study_sync] "
                f"case_study_id {case_study_id}, "
                f"case_study_name {case_study_name} handled successfully!")

    return (
        etl_request, 
        new_etl_case_study, 
        etl_case_study_data_list, 
        recrawl_ids, 
        recrawl_etl_company_datasources
    )


def extract_company_datasource_id_from_case_study_payload(payload):
    """
    Extract payload for company_datasources in case_study
    """
    company_datasources = payload['company_datasources']
    re_scrape_ids = []
    all_ids = []
    for company_datasource in company_datasources:
        if company_datasource['re_scrape']:
            re_scrape_ids.append(company_datasource['company_datasource_id'])
        all_ids.append(company_datasource['company_datasource_id'])

    return all_ids, re_scrape_ids

def construct_existing_company_datasource_payloads(company_datasource_ids, data_type):
    """
    Construct a valid company_datasource payload from a list of company_datasource_id
    Company datasource id must exists in etl_company_datasource to construct a payload
    """
    assert len(company_datasource_ids) > 0, "company_datasource_ids cannot be empty"
    logger.info(f"[construct_payload] construct payloads"
            f"for {len(company_datasource_ids)} ids: {company_datasource_ids}")
    ## GET company_datasource with review type
    etl_company_datasources = []
    etl_company_datasources_hra = []
    if data_type != config.HRA_DATA_TYPE:
        etl_company_datasources = postgres_company_datasource.get_etl_company_datasource_by_ids(company_datasource_ids, config.REVIEW_DATA_TYPE)
        assert len(etl_company_datasources) > 0, "at least 1 company_datasource_id must exist in database"
    else:
        etl_company_datasources_hra = postgres_company_datasource.get_etl_company_datasource_by_ids(company_datasource_ids, config.CORESIGNAL_EMPLOYEES_DATA_TYPE)
        assert len(etl_company_datasources_hra) > 0, "at least 1 company_datasource_id must exist in database"
    ## GET company_datasource with job type
    etl_company_datasource_jobs = []
    if data_type == config.VOE_DATA_TYPE:
        etl_company_datasource_jobs = postgres_company_datasource.get_etl_company_datasource_by_ids(company_datasource_ids, config.JOB_DATA_TYPE)
        assert len(etl_company_datasource_jobs) > 0, "at least 1 company_datasource_id in job must exist in database"
    payloads = []
    ## GENERATE PAYLOAD WITH REVIEW TYPE
    logger.info("[construct_payload] GENERATE PAYLOAD WITH REVIEW TYPE")
    for etl_company_datasource in etl_company_datasources:
        etl_company_datasource = etl_company_datasource.to_json()
        url_review = list(filter(lambda url: url['type'] == config.REVIEW_DATA_TYPE, etl_company_datasource['urls']))
        # generate 2 payloads, 
        # one for `review` 
        payload = generate_payload(etl_company_datasource, config.REVIEW_DATA_TYPE, url_review)
        logger.info(f"[construct_review_payload] payloads constructed {payload}")
        payloads.append(payload)
        # and one for `review_stats`
        payload_review_stats = generate_payload(etl_company_datasource, config.REVIEW_STATS_DATA_TYPE, url_review)
        logger.info(f"[construct_review_payload] payloads constructed {payload_review_stats}")
        payloads.append(payload_review_stats)

    ## GENERATE PAYLOAD WITH JOB TYPE
    logger.info("[construct_payload] GENERATE PAYLOAD WITH JOB TYPE")
    for etl_company_datasource_job in etl_company_datasource_jobs:
        etl_company_datasource_job = etl_company_datasource_job.to_json()
        urls_job_overview = list(filter(lambda url: url['type'] != config.REVIEW_DATA_TYPE, etl_company_datasource_job['urls']))
        payload = generate_payload(etl_company_datasource_job, config.JOB_DATA_TYPE, urls_job_overview)
        logger.info(f"[construct_job_payload] payloads constructed {payload}")
        payloads.append(payload)

    ## GENERATE PAYLOAD WITH HRA TYPE
    logger.info("[construct_payload] GENERATE PAYLOAD WITH HRA TYPE")
    for etl_company_datasource in etl_company_datasources_hra:
        etl_company_datasource = etl_company_datasource.to_json()
        payload = generate_payload(etl_company_datasource, "coresignal_stats", etl_company_datasource['urls'])
        logger.info(f"[construct_hra_payload] payloads constructed {payload}")
        payloads.append(payload)
        payload = generate_payload(etl_company_datasource, "coresignal_employees", etl_company_datasource['urls'])
        logger.info(f"[construct_hra_payload] payloads constructed {payload}")
        payloads.append(payload)
    logger.info(f"[construct_payload] number of construct payloads {len(payloads)}")
    return payloads

def generate_payload(etl_company_datasource, data_type, urls):
    payload = {
        "type": config.COMPANY_DATASOURCE_REQUEST_TYPE,
        "data_type": data_type,
        "company_datasource_id": etl_company_datasource['company_datasource_id'],
        "company_id": etl_company_datasource['company_id'],
        "company_name": etl_company_datasource['company_name'],
        "source_id": etl_company_datasource['source_id'],
        "source_name": etl_company_datasource['source_name'],
        "source_code": etl_company_datasource['source_code'],
        "source_type": etl_company_datasource['source_type'],
        "urls": urls,
        "nlp_type": etl_company_datasource['nlp_type'],
        "translation_service": config.GOOGLE_TRANSLATION_SERVICE
    }
    return payload


def handle_recrawl(recrawl_ids, recrawl_payloads):
    recrawl_etl_company_datasources = []
    for _payload in recrawl_payloads:
        try:
            company_datasource_id = _payload['company_datasource_id']
            data_type = _payload["data_type"]
            etl_company_datasource = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=company_datasource_id,
                data_type=data_type,
            )
            _, updated_etl_company_datasource, _ = handlers_company_datasource.handle_company_datasource_exists(
                payload=_payload,
                data_type=data_type,
                current_etl_company_datasource=etl_company_datasource,
            )
            # Purpose: to have request ids to send to company_datasource_orchestrator later
            # If this payload is handed successfully, then the update etl_company_datasource
            # entry will be available and added into crawl ids.
            # If any exception happens, as catched below, will not append
            recrawl_etl_company_datasources.append((updated_etl_company_datasource, data_type))
        except Exception as e:
            # update fails
            if isinstance(e, CompanyExistsAndRunningError):
                logger.exception("[handle_postgres_case_study_sync] handle re-scrape request fails, "
                                f"will add company datasource as-is")
            else:
                logger.exception("[handle_postgres_case_study_sync] handle re-scrape request fails, "
                                f"will add company datasource previous version")
                recrawl_ids.remove(_payload['company_datasource_id'])
    return recrawl_ids, recrawl_payloads, recrawl_etl_company_datasources


def get_company_datasources(payload_company_datasources, nlp_type: str):
    company_datasource_all = []
    for payload_company_datasource in payload_company_datasources:
        
        if nlp_type.lower() in (config.VOC_DATA_TYPE.lower(), config.VOE_DATA_TYPE.lower()):
            # review part
            company_datasource = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=payload_company_datasource['company_datasource_id'],
                data_type=config.REVIEW_DATA_TYPE
            )
            company_datasource_all.append((
                company_datasource, 
                config.REVIEW_DATA_TYPE, 
                payload_company_datasource))
            # review stats
            company_datasource_review_stats = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=payload_company_datasource['company_datasource_id'],
                data_type=config.REVIEW_STATS_DATA_TYPE
            )
            company_datasource_all.append((
                company_datasource_review_stats,
                config.REVIEW_STATS_DATA_TYPE,
                payload_company_datasource
            ))

        if nlp_type.lower() == config.VOE_DATA_TYPE.lower():
            company_datasource = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=payload_company_datasource['company_datasource_id'],
                data_type=config.JOB_DATA_TYPE
            )
            if company_datasource:
                company_datasource_all.append((company_datasource, config.JOB_DATA_TYPE, payload_company_datasource))
                
        if nlp_type.lower() == config.HRA_DATA_TYPE.lower():
            # stats/company profile
            company_datasource_coresignal_stats = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=payload_company_datasource['company_datasource_id'],
                data_type=config.CORESIGNAL_STATS_DATA_TYPE
            )
            company_datasource_all.append((
                company_datasource_coresignal_stats,
                config.CORESIGNAL_STATS_DATA_TYPE,
                payload_company_datasource
            ))
            # employee profile
            company_datasource_coresignal_employees = postgres_company_datasource.get_etl_company_datasource(
                company_datasource_id=payload_company_datasource['company_datasource_id'],
                data_type=config.CORESIGNAL_EMPLOYEES_DATA_TYPE
            )
            company_datasource_all.append((
                company_datasource_coresignal_employees,
                config.CORESIGNAL_EMPLOYEES_DATA_TYPE,
                payload_company_datasource
            ))
        
    return company_datasource_all
## END SYNC


### BEGIN DIMENSION CHANGE
def handle_case_study_dimension_change(payload):
    """
    Handle postgres updates for dimension change:
    - check cs existence
    - update case_study_status
    """
    case_study_id = payload['case_study_id']
    case_study_name = payload['case_study_name']
    logger.info(f"[handle_postgres_case_study_dimension_change] start handling "
                f"case_study_id {case_study_id}, "
                f"case_study_name {case_study_name}")

    # I. check case study existence
    etl_case_study = postgres_case_study.get_etl_case_study(case_study_id=case_study_id)

    if not etl_case_study:
        raise Exception("[handle_postgres_dimension_change_case_study] "
                        f"case_study_id {case_study_id} does not exist. "
                        "Request rejected!")

    # II. update etl_case_study status, progress (set to 0.5), and payload
    updated_etl_case_study = postgres_case_study.reset_etl_case_study_status_and_progress(
        case_study_id=case_study_id,
        payload=payload,
    )
    etl_case_study_data_as_dict = get_etl_case_study_data_for_case_study_as_dict(
        case_study_id=case_study_id,
    )
    etl_case_study_data_list = list(etl_case_study_data_as_dict.values())
    request_id = updated_etl_case_study['request_id']
    etl_request = postgres_common.get_etl_request(request_id=request_id)

    logger.info(f"[handle_postgres_case_study_dimension_change] "
                f"case_study_id {case_study_id}, "
                f"case_study_name {case_study_name} handled successfully!")

    return etl_request, updated_etl_case_study, etl_case_study_data_list


def get_etl_case_study_data_for_case_study_as_dict(case_study_id):
    """
    Get all etl_case_study_data entries for a given case_study_id
    """
    etl_case_study_data_list = postgres_case_study.get_etl_case_study_data_by_cs_id(case_study_id)
    results = {}
    for etl_case_study_data in etl_case_study_data_list:
        _dict = etl_case_study_data.to_json()
        results.update({
            _dict['company_datasource_id']: _dict
        })

    if results:
        logger.info(f"[get_etl_case_study_data_for_case_study_as_dict] "
                    f"case_study_id {case_study_id} has {len(results)} company_datasources")
        return results


### END DIMENSION CHANGE

### START UPDATE
def handle_case_study_update(payload):
    """
    Handle postgres updates for updating case_study_data
    - check cs existence
    - compare data_version of
        - company_datasource in etl_case_study_data
        - company_datasource in etl_company_datasource
    - update etl_case_study_data metadata if necessary
    """
    case_study_id = payload['case_study_id']
    case_study_name = payload['case_study_name']
    logger.info(f"[handle_postgres_case_study_update] start handling "
                f"case_study_id {case_study_id}, "
                f"case_study_name {case_study_name}")

    # I. check case study existence
    etl_case_study = postgres_case_study.get_etl_case_study(case_study_id=case_study_id)

    if not etl_case_study:
        raise Exception("[handle_postgres_update_case_study]: "
                        f"case_study_id {case_study_id} does not exist. "
                        "Request to update rejected!")
    request_id = etl_case_study['request_id']
    etl_request = postgres_common.get_etl_request(request_id=request_id)
    # update case study status
    updated_etl_case_study = postgres_case_study.reset_etl_case_study_status_and_progress(
        case_study_id=case_study_id,
        payload=payload,
    )

    # II. compare data_version
    # 1. get case study data
    etl_case_study_data_dict = get_etl_case_study_data_for_case_study_as_dict(
        case_study_id=case_study_id,
    )
    # 2. get etl company datasources
    etl_company_datasources = [
        (_id, _etl_case_study_data["data_type"]) 
        for _id, _etl_case_study_data
        in etl_case_study_data_dict.items()
    ]

    etl_company_datasources_dict = postgres_company_datasource.get_etl_company_datasources_as_dict(
        etl_company_datasources=etl_company_datasources,
    )
    # 3. update data_version
    # if company_datasource data_version in storage is newer than data_version
    # in case study, AND company_datasource in storage is NOT running, then update the
    # case study data_version to storage's version.
    # In case the storage is running, then only update to the previous (storage) data_version
    # if that previous version is still newer than case_study's data_version
    updated_case_study_data_list = []
    full_case_study_data_list = []
    for company_datasource_id, data_type in etl_company_datasources:
        _current_key = (company_datasource_id, data_type)
        case_study_data_version = etl_case_study_data_dict[_current_key]['data_version']
        in_storage_data_version = etl_company_datasources_dict[_current_key]['data_version']
        in_storage_company_datasource_status = etl_company_datasources_dict[_current_key]['status']
        need_update_data_version = True
        data_version = case_study_data_version
        # Update version data_version
        if case_study_data_version < in_storage_data_version:
            if in_storage_company_datasource_status in (config.FINISHED_STATUS, 'completed with error'):
                data_version = in_storage_data_version
            elif case_study_data_version < in_storage_data_version - 1: # storage is running
                data_version = in_storage_data_version - 1
            else: # both wrong, will not update
                need_update_data_version = False
        # update version in case study if needed
        if need_update_data_version:
            _updated_case_study_data = postgres_case_study.update_etl_case_study_data_version(
                case_study_id=case_study_id,
                company_datasource_id=company_datasource_id,
                data_version=data_version,
                data_type=data_type,
                logger=logger
            )
            if _updated_case_study_data:
                _updated_case_study_data['previous_data_version'] = case_study_data_version
            # append for counting
            updated_case_study_data_list.append(_updated_case_study_data)
        
        # append back to a full list for downstream tasks
        full_case_study_data_list.append({
            'company_datasource_id': company_datasource_id,
            'data_type': data_type,
            'data_version': data_version,
            'previous_data_version': case_study_data_version
        })
            

    logger.info(f"[handle_postgres_case_study_update] case_study_id {case_study_id} "
                f"update handled successfully: "
                f"data_version of "
                f"{len(updated_case_study_data_list)}/{len(full_case_study_data_list)} "
                f"company_datasources updated")

    return etl_request, etl_case_study, full_case_study_data_list

### END UPDATE
def publish_pubsub_case_study(request_id, case_study_id, action, cs_data_version_changes):
    # send progress -1.0 to progress topic to acknowledge request
    # reconstruct data version to send to webapp
    mapping_datasource_id = {}
    for dvc in cs_data_version_changes:
        sample_dvc_output = {
            "company_datasource_id": -1,
            "data_version_review": -1,
            "data_version_job": -1,
            "data_version_coresignal_employees": -1,
        }
        company_datasource_id = dvc["company_datasource_id"]
        if not mapping_datasource_id.get(company_datasource_id):
            mapping_datasource_id[company_datasource_id] = sample_dvc_output
            mapping_datasource_id[company_datasource_id]["company_datasource_id"] = company_datasource_id
        if dvc["data_type"] == "review":
            mapping_datasource_id[company_datasource_id]["data_version_review"] = dvc["new_data_version"]
        if dvc["data_type"] == "job":
            mapping_datasource_id[company_datasource_id]["data_version_job"] = dvc["new_data_version"]
        if dvc["data_type"] == "coresignal_employees":
            mapping_datasource_id[company_datasource_id]["data_version_coresignal_employees"] = dvc["new_data_version"]

    data_versions = list(mapping_datasource_id.values())
    logger.info(f"[publish_pubsub_case_study] push message to WEBAPP: \
                {data_versions}")
    progress_payload = {
        'event': config.PROGRESS_EVENT,
        'case_study_id': case_study_id,
        'data_versions': data_versions,
        'progress': config.PROGRESS
    }
    pubsub_util.publish(
        logger,
        config.GCP_PROJECT_ID,
        config.GCP_PUBSUB_TOPIC_CS_PROGRESS,
        progress_payload
    )
    # send message to data_flow
    data_flow_payload = {
        'request_id': request_id,
        'case_study_id': case_study_id,
        'action': action,
        'case_study_data_version_changes': cs_data_version_changes,
        'event': config.DATA_CONSUME_EVENT
    }
    pubsub_util.publish(
        logger,
        config.GCP_PROJECT_ID,
        config.GCP_PUBSUB_TOPIC_DATA_CONSUME_DATA_FLOW,
        data_flow_payload
    )
