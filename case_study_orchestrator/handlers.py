from core import pubsub_util
import uuid
import sys
sys.path.append('../')

import config
import postgres_helpers


def handle_case_study_sync(payload, logger):
    """
    Handle data_flow request for a new case study:
    - Just create new etl_postprocess steps
    - assign run_ids
    - update case study status and progress from waiting (created by websync) to running
    - will not need to publish anything, case_study_company_datasource_updater listener will pick up and run
    
    For a case_study 'update' request, the entire process should be re-done so processing is identical
    """
    # get relevant info
    request_id = payload['request_id']
    case_study_id = payload['case_study_id']
    action = payload['action']
    cs_data_version_changes = payload['case_study_data_version_changes']
    
    # initiate new postprocess steps
    run_id = str(uuid.uuid1())
    meta_data = {
        'version': '2.0',
        'run_id': run_id,
        'case_study_data_version_changes': cs_data_version_changes,
    }
    for postprocess_type in config.NEXT_STEP_TYPES.keys():
        logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                    f"creating postprocess step '{postprocess_type}'...")
        postgres_helpers.create_etl_postprocess(
            request_id=request_id,
            case_study_id=case_study_id,
            status='waiting',
            progress=0.0,
            postprocess_type=postprocess_type,
            meta_data=meta_data,
            logger=logger
        )
    
    return run_id


def handle_case_study_update(payload, logger):
    """
    same handle as sync. Leave a separate function here so that if anything needs to be changes, it 
    can be changed here.
    """
    # get relevant info
    request_id = payload['request_id']
    case_study_id = payload['case_study_id']
    action = payload['action']
    cs_data_version_changes = payload['case_study_data_version_changes']
    
    # initiate new postprocess steps
    run_id = str(uuid.uuid1())
    meta_data = {
        'version': '2.0',
        'run_id': run_id,
        'case_study_data_version_changes': cs_data_version_changes,
    }
    
    for postprocess_type in config.NEXT_STEP_TYPES.keys():
        logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                    f"creating postprocess step '{postprocess_type}'...")
        postgres_helpers.create_etl_postprocess(
            request_id=request_id,
            case_study_id=case_study_id,
            status='waiting',
            progress=0.0,
            postprocess_type=postprocess_type,
            meta_data=meta_data,
            logger=logger
        )


def handle_case_study_dimension_change(payload, logger):
    """
    Handle data_flow request for dimension_change of an existing case study
    - create new etl_postprocess steps:
        - Upon creation, set update_cs_data, nlp, and load_cs_data as finished, progress 1.0
        - All remaining are waiting and 0.0 progress
    - update case study status and progress from finished to running
    - send 'load_cs_data' finished message to aftertask to trigger the 'keyword_extract' step
    """
    # get relevant info
    request_id = payload['request_id']
    case_study_id = payload['case_study_id']
    action = payload['action']
    cs_data_version_changes = payload['case_study_data_version_changes']
    
    # initiate new postprocess steps
    run_id = str(uuid.uuid1())
    meta_data = {
        'version': '2.0',
        'run_id': run_id,
        'case_study_data_version_changes': cs_data_version_changes,
    }
    postprocess_dict = {}
    for postprocess_type in config.NEXT_STEP_TYPES.keys():
        # only changing dimesnion config on prepared data.
        if postprocess_type in config.CASE_STUDY_PREPARATION_STEPS:
            step_status = 'finished'
            step_progress = 1.0
        else:
            step_status = 'waiting'
            step_progress = 0.0
        # create postprocess steps
        logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                    f"creating postprocess step '{postprocess_type}', status '{step_status}'")
        etl_postprocess = postgres_helpers.create_etl_postprocess(
            request_id=request_id,
            case_study_id=case_study_id,
            postprocess_type=postprocess_type,
            status=step_status,
            progress=step_progress,
            meta_data=meta_data,
            logger=logger
        )
        postprocess_dict[postprocess_type] = etl_postprocess
    
    # send after-task finished of load_cs_data step to after_task listener 
    # so that keyword_extraction step can be triggered
    load_cs_data_after_task_payload = {
        "type": "load_cs_data",
        "event": "finish",
        "request_id": request_id,
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_dict["load_cs_data"]["postprocess_id"],
        "postprocess": postprocess_dict["load_cs_data"],
        "error": "",
    }
    pubsub_util.publish(
        gcp_product_id=config.GCP_PROJECT,
        topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
        payload=load_cs_data_after_task_payload,
        logger=logger
    )
    
    return run_id
    
    
def handle_case_study_clone(payload, logger):
    """
    Handle data_flow request for a new case study, but cloned:
    - deprecate all old steps
    - create new etl_postprocess steps:
        - Upon creation, set update_cs_data, nlp as finished, progress 1.0
        - All remaining are waiting and 0.0 progress
    => same as dimension_change
    - send nlp finish message to after_task
    - after_task will trigger load_cs_data, where cloning bigquery data is handled
    """
    request_id = payload['request_id']
    case_study_id = payload['case_study_id']
    action = payload['action']
    
    # initiate new postprocess steps
    run_id = str(uuid.uuid1())
    meta_data = {
        'version': '2.0',
        'run_id': run_id,
    }
    postprocess_id_dict = {}
    for postprocess_type in config.NEXT_STEP_TYPES.keys():
        # for cloned cs, update_cs_data and nlp has been completed
        if postprocess_type in ('update_cs_data', 'nlp'):
            step_status = 'finished'
            step_progress = 1.0
        else:
            # load_cs_data will handle cloning old case study data into new case study and run calculation steps
            step_status = 'waiting'
            step_progress = 0.0
        # create postprocess steps
        logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                    f"creating postprocess step '{postprocess_type}', status '{step_status}'")
        etl_postprocess = postgres_helpers.create_etl_postprocess(
            request_id=request_id,
            case_study_id=case_study_id,
            postprocess_type=postprocess_type,
            status=step_status,
            progress=step_progress,
            meta_data=meta_data,
            logger=logger
        )
        postprocess_id_dict[postprocess_type] = etl_postprocess['postprocess_id']
    
    # send after-task finished of nlp step to after_task listener so that load_cs_data can be triggered
    nlp_after_task_payload = {
        "type": "nlp",
        "event": "finish",
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_id_dict["nlp"],
        "error": "",
    }
    pubsub_util.publish(
        gcp_product_id=config.GCP_PROJECT,
        topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
        payload=nlp_after_task_payload,
        logger=logger
    )
    
    return run_id


def handle_progress_update_case_study_finish(payload, logger):
    """
    Update progress for case study, progress steps, and announce progress
    when case study is finished
    """
    case_study_id = payload['case_study_id']
    # get all active postprocess steps of this case study id
    active_etl_preprocesses = postgres_helpers.get_active_etl_postprocesses(
        case_study_id=case_study_id,
        logger=logger
    )
    # extract progress detail of each step. These are updated since cs is finished
    progress_details = {}
    for step_dict in active_etl_preprocesses:
        progress_details[step_dict['postprocess_type']] = step_dict['progress']
    
    # determine status. If any postprocess step is completed with error, CS will be so as well
    statuses = list(map(lambda x: x['status'], active_etl_preprocesses))
    if 'completed with error' in statuses:
        case_study_status = 'completed with error'
    else:
        case_study_status = 'finished'

    # case study is finished, change CS status and set progress to 1.0
    updated_etl_case_study = postgres_helpers.update_etl_case_study(
        case_study_id=case_study_id,
        status=case_study_status,
        progress=1.0,
        logger=logger
    )
    # also set request_id status to be the same as case_study_status
    updated_etl_request = postgres_helpers.update_etl_request_status(
        request_id=updated_etl_case_study['request_id'],
        status=case_study_status,
        logger=logger
    )
    
    # construct progress payload
    web_progress_payload = {
        'event': 'progress',
        'case_study_id': case_study_id,
        'progress': 1.0,
        'details': progress_details
    }
    
    return web_progress_payload


def handle_progress_update_case_study_postprocess_progress(payload, logger):
    """
    Update progress for case study, progress steps, and announce progress
    for a postprogress step progress announcement
    """
    # extract info
    postprocess_type = payload['type']
    postprocess_id = payload['postprocess_id']
    case_study_id = payload['case_study_id']
    new_progress_value = payload['progress']
    
    # get info of current case study
    etl_case_study = postgres_helpers.get_etl_case_study(
        case_study_id=case_study_id,
        logger=logger
    )
    
    # get all active postprocess steps of this case study id
    active_etl_postprocess_steps = postgres_helpers.get_active_etl_postprocesses(
        case_study_id=case_study_id,
        logger=logger
    )
    
    # extract progress detail of each step. These are updated since cs is finished
    progress_details = {}
    sum_progress = 0.0
    for step_dict in active_etl_postprocess_steps:
        progress_details[step_dict['postprocess_type']] = step_dict['progress']
        sum_progress += step_dict['progress']
    
    # check progress value of current step, only update if progress is made
    old_progress_value = progress_details[postprocess_type]
    if new_progress_value > old_progress_value:
        progress_details[postprocess_type] = new_progress_value
        # update the current etl_progress step
        etl_postprocess = postgres_helpers.update_etl_postprocess(
            postprocess_id=postprocess_id,
            progress=new_progress_value,
            logger=logger
        )
        
    # calculate new case study progress
    new_case_study_progress = min(sum_progress / len(active_etl_postprocess_steps), 0.99)
    # update case study status, only update if new progress is made
    if new_case_study_progress > etl_case_study['progress']:
        updated_etl_case_study = postgres_helpers.update_etl_case_study(
            case_study_id=case_study_id,
            progress=new_case_study_progress,
            logger=logger
        )
    else:
        new_case_study_progress = etl_case_study['progress']
        
    # construct progress payload
    web_progress_payload = {
        'event': 'progress',
        'case_study_id': case_study_id,
        'progress': new_case_study_progress,
        'details': progress_details
    }
    
    return web_progress_payload
