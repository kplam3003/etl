from copy import deepcopy
import json
import time
import datetime as dt
import sys
import uuid
sys.path.append('../')

from google.cloud import pubsub_v1

import config
import postgres_helpers
import handlers
from core import pubsub_util

from core.logger import auto_logger
from database import (
    auto_session,
    ETLCompanyDatasourceCommon,
)


@auto_logger('data_consume_data_flow_listener')
def data_consume_data_flow_listener(logger=None):
    """
    Listen to data_consume message from data_flow topic:
    - Set the case study message status to 'running'
    - Check if there are old ETLPostprocess
        => if there are, set 'is_deprecated' to True 
    - Initialize all ETLPostprocess steps to waiting
    """
    logger.info("[data_consume_data_flow_listener] starts listening for data_consume message...")
    
    project_id = config.GCP_PROJECT
    data_flow_subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_DATA_FLOW
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, data_flow_subscription_id)
    
    with subscriber:
        while True:
            logger.info(f"[data_consume_data_flow_listener] listening on {data_flow_subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )
            
            if not response:
                time.sleep(config.SLEEP)
                continue
                
            for received_message in response.received_messages:
                logger.info(f"[data_consume_data_flow_listener] received: {received_message.message.data}")
                received_payload = json.loads(received_message.message.data)
                
                try:
                    # get relevant info
                    request_id = received_payload['request_id']
                    case_study_id = received_payload['case_study_id']
                    action = received_payload['action']
                    etl_case_study = postgres_helpers.get_etl_case_study(
                        case_study_id=case_study_id,
                        logger=logger
                    )
                    case_study_payload = etl_case_study['payload']
                    
                    # try to deprecate previous postprocess steps if any
                    if action != 'sync':
                        logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                                    "deprecating postprocess steps from previous run")
                        postgres_helpers.deprecate_old_etl_postprocess(case_study_id=case_study_id, logger=logger)
                    
                    # update status and progress of requested case study to running
                    postgres_helpers.update_etl_case_study_status_and_progress(
                        case_study_id=case_study_id,
                        status='running',
                        progress=0.0,
                        logger=logger
                    )
                    postgres_helpers.update_etl_request_status( # also update etl request, legacy purpose
                        request_id=request_id,
                        status='running',
                        logger=logger
                    )
                    
                    logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}: "
                                f"action {action} received.")

                    if action == 'sync':
                        run_id = handlers.handle_case_study_sync(payload=received_payload, logger=logger)
                        
                    elif action == 'update':
                        run_id = handlers.handle_case_study_update(payload=received_payload, logger=logger)
                          
                    elif action == 'dimension_change':
                        run_id = handlers.handle_case_study_dimension_change(payload=received_payload, logger=logger)
                    
                    else:
                        raise Exception(f"[data_consume_data_flow_listener] case_study_id {case_study_id}"
                                        f"action {action} not supported")
                    
                    # acknowledge the message
                    logger.info(f"[data_consume_data_flow_listener] case_study_id {case_study_id}, "
                                f"action {action} handled successfully! Acknowledging the message...")
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
                    )
                
                except Exception as e:
                    logger.exception(f"[data_consume_data_flow_listener] unable to process received message!")


@auto_logger('case_study_company_datasource_updater')
def case_study_company_datasource_updater(logger=None):
    """
    check case study data status:
    - query not finished etl_case_study_data to get company_datasource_id and data_version
    - query these company_datasource_id in etl_company_datasource
        - if data_version in etl_company_datasource is larger than that of etl_case_study_data
            => set etl_case_study_data to finished, progress to 1.0
        - if data_version is equal
            => update status and progress to that in etl_company_datasource
    - check case_studies of just-update case_study_data:
        - if just-updated case_study has all case_study_data finished/completed with error
            => send finished message to after_task
        - if not, calculate average progress of all case_study_data of a case study
            => send message to case study internal progress
            => update into ETLPostprocess, step name 'update_cs_data'
    """
    POSTPROCESS_TYPE = 'update_cs_data'
    while True:
        # check for unfinished update_cs_data postprocess step
        logger.info(f"[case_study_data_updater] checking for unfinished update_cs_data postprocess step...")
        running_update_cs_data_steps = postgres_helpers.get_unfinished_etl_postprocess_step_update_cs_data(
            logger=logger
        )
        if not running_update_cs_data_steps:
            logger.info(f"[case_study_data_updater] no running update_cs_data step found, going back to sleep...")
            time.sleep(config.SLEEP)
            continue
        
        # if there is, starts handling
        # I. get CS ids with running update_cs_data step
        running_case_study_id_dict = {}
        for cs_data in running_update_cs_data_steps:
            running_case_study_id_dict[cs_data['case_study_id']] = cs_data['postprocess_id']
        
        # II. check and update case_study_data companies to its latest status in etl_company_datasource
        # 1. check for currently unfinished etl_case_study_data
        logger.info(f"[case_study_data_updater] checking for unfinished case_study_data...")
        unfinished_etl_case_study_data_list = postgres_helpers.get_unfinished_etl_case_study_data(logger=logger)
        unfinished_cs_company_datasource_ids = set()
        for csd in unfinished_etl_case_study_data_list:
            # tuple of structure: (company_datasource_id, data_type)
            tup = (csd['company_datasource_id'], csd['data_type'])
            unfinished_cs_company_datasource_ids.add(tup)
        # 2. try to update to the latest status in etl_company_datasource
        updated_case_study_ids, updated_company_datasource_ids = \
            postgres_helpers.update_etl_case_study_data_to_in_storage_status(
                company_datasource_ids=unfinished_cs_company_datasource_ids, 
                logger=logger
            )
            
        # III. calculate progress for updated case studies
        # get updated cs's statuses and progersses
        for case_study_id, postprocess_id in running_case_study_id_dict.items():
            logger.info("Calculate progress for updated case studies")
            try:
                etl_postprocess = postgres_helpers.get_active_etl_postprocess_by_case_study_and_type(
                    case_study_id=case_study_id,
                    postprocess_type=POSTPROCESS_TYPE,
                    logger=logger
                )
                cs_data_statuses_set, cs_data_progresses = \
                    postgres_helpers.get_etl_case_study_data_status_and_progress(
                        case_study_id=case_study_id,
                        logger=logger
                    )
                # check if finished
                cs_data_statuses_intersection = cs_data_statuses_set.intersection(config.RUNNING_STATUSES)
                if cs_data_statuses_intersection: # still unfinished
                    # calculate progress
                    cs_data_progress = sum(cs_data_progresses) / len(cs_data_progresses)
                    progress_payload = {
                        "type": POSTPROCESS_TYPE,
                        "event": "progress",
                        "request_id": etl_postprocess['request_id'],
                        "case_study_id": case_study_id,
                        "postprocess_id": etl_postprocess['postprocess_id'],
                        "progress": cs_data_progress
                    }
                    pubsub_util.publish(
                        gcp_product_id=config.GCP_PROJECT,
                        topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                        payload=progress_payload,
                        logger=logger,
                    )
                    logger.info(f"[case_study_data_updater] case_study_id {case_study_id} "
                                f"{POSTPROCESS_TYPE} progress {cs_data_progress}")
                
                else: # all finished
                    # send aftertask
                    logger.info(f"[case_study_data_updater] case_study_id {case_study_id} "
                                f"{POSTPROCESS_TYPE} finished")
                    error_text = ''
                    aftertask_payload = {
                        "type": POSTPROCESS_TYPE,
                        "event": "finish",
                        "case_study_id": case_study_id,
                        "request_id": etl_postprocess['request_id'],
                        "postprocess_id": postprocess_id,
                        "postprocess": etl_postprocess,
                        "error": error_text,
                    }
                    pubsub_util.publish(
                        gcp_product_id=config.GCP_PROJECT,
                        topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
                        payload=aftertask_payload,
                        logger=logger,
                    )

                    
            except Exception as e:
                logger.exception(f"[case_study_data_updater] case_study_id {case_study_id} "
                                f"{POSTPROCESS_TYPE} failed")
                error_text = f'{e}'[:200]
                aftertask_payload = {
                    "type": POSTPROCESS_TYPE,
                    "event": "fail",
                    "case_study_id": case_study_id,
                    "request_id": etl_postprocess['request_id'],
                    "postprocess_id": postprocess_id,
                    "postprocess": etl_postprocess,
                    "error": error_text,
                }
                pubsub_util.publish(
                    gcp_product_id=config.GCP_PROJECT,
                    topic=config.GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK,
                    payload=aftertask_payload,
                    logger=logger,
                )

        logger.info(f"[case_study_data_updater] finished, going back to sleep... ")
        time.sleep(config.SLEEP)


@auto_session
@auto_logger('case_study_after_task_listener')
def case_study_after_task_listener(session=None, logger=None):
    """
    Listen for status updates of ETLPostprocess steps. When receives a message:
    - if event is 'finish'
        => update ETLPostprocess step status to finish and progress to 1.0
    - if event is 'fail'
        => update ETLPostprocess step status to completed with error and progress to 1.0
    => send progress 1.0 update of ETLPostprocess step to internal progress topic
    => prepare payload and send to the next ETLPostprocess step
    """
    aftertask_receive_sample_payload = {
        "type": "POSTPROCESS_TYPE",
        "event": "fail",
        "case_study_id": "case_study_id",
        "postprocess_id": "etl_postprocess['postprocess_id']",
        "error": "error_text",
    }
    aftertask_send_sample_payload = {
        'type': "next_postprocess_type",
        'case_study_id': "case_study_id",
        'request_id': "etl_case_study['request_id']",
        'status': "etl_case_study['status']",
        'payload': "case_study_payload",
        'run_id': "run_id",
        'postprocess': "next_etl_postprocess"
    }
    
    logger.info("[case_study_after_task_listener] starts listening for after_task messages...")
    
    project_id = config.GCP_PROJECT
    after_task_subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_AFTER_TASK
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, after_task_subscription_id)
    
    with subscriber:
        while True:
            logger.info(f"[case_study_after_task_listener] listening on {after_task_subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )
            
            if not response:
                time.sleep(config.SLEEP)
                continue
                
            for received_message in response.received_messages:
                logger.info(f"[case_study_after_task_listener] received: {received_message.message.data}")
                received_payload = json.loads(received_message.message.data)
                
                try:
                    # extract infos
                    postprocess_type = received_payload['type']
                    postprocess_status = config.EVENT_TO_STATUS.get(received_payload['event'])
                    case_study_id = received_payload['case_study_id']
                    if 'postprocess' in received_payload:
                        postprocess_id = received_payload['postprocess']['postprocess_id']
                    else:
                        raise Exception(f"[case_study_after_task_listener] payload has no postprocess object")
                    
                    postprocess_error = received_payload['error'][:200] # limit to 200 character for error text
                    logger.info(f"[case_study_after_task_listener] postprocess {postprocess_id} - {postprocess_type} - "
                                f"case_study_id {case_study_id} has status {postprocess_status}")
                    # update etl_postprocess status
                    etl_postprocess = postgres_helpers.update_etl_postprocess(
                        postprocess_id=postprocess_id,
                        status=postprocess_status,
                        postprocess_error=postprocess_error,
                        logger=logger        
                    )

                    # prepare for next step
                    # update status in DB from 'waiting' to 'running'
                    next_postprocess_type = config.NEXT_STEP_TYPES[postprocess_type]
                    if next_postprocess_type != config.CASE_STUDY_FINISHED_TYPE:
                        # case study still has next step, trigger next step
                        logger.info(f"[case_study_after_task_listener] triggering next step: "
                                    f"{next_postprocess_type} - {postprocess_id} - case_study_id {case_study_id}")
                        # cs is not done
                        next_etl_postprocess = postgres_helpers.update_active_etl_postprocess_by_type_and_case_study_id(
                            case_study_id=case_study_id,
                            postprocess_type=next_postprocess_type,
                            status='running',
                            logger=logger
                        )
                        # prepare payload
                        run_id = next_etl_postprocess['meta_data']['run_id']
                        cs_data_version_changes = next_etl_postprocess['meta_data'].get('case_study_data_version_changes', [])
                        etl_case_study = postgres_helpers.get_etl_case_study(
                            case_study_id=case_study_id,
                            logger=logger
                        )
                        etl_case_study_data_dict = postgres_helpers.get_etl_case_study_data_for_case_study_as_dict(
                            case_study_id=case_study_id,
                            logger=logger
                        )
                        # extract needed infos from etl_case_study_data_dict:
                        case_study_payload = etl_case_study['payload']
                        
                        # update company_datasources field in case study payload with information from mdm
                        # NOTE: not necessary anymore.
                        # for company_datasource_dict in case_study_payload['company_datasources']:
                        #     _id = company_datasource_dict['company_datasource_id']
                        #     # add data_version
                        #     company_datasource_dict['data_version'] = etl_case_study_data_dict[_id]['data_version']
                        #     company_datasource_dict['data_version_job'] = etl_case_study_data_dict[_id]['data_version_job']
                        #     company_datasource_dict['urls'] = etl_company_datasources_dict[_id]['urls']
                        
                        next_postprocess_payload = {
                            'type': next_postprocess_type,
                            'case_study_id': case_study_id,
                            'request_id': etl_case_study['request_id'],
                            'status': etl_case_study['status'],
                            'payload': case_study_payload,
                            'run_id': run_id,
                            'case_study_data_version_changes': cs_data_version_changes,
                            'postprocess': next_etl_postprocess
                        }
                        # only needed for keyword_extract step. If force is True, delete old results and re-parse
                        if next_postprocess_type == 'keyword_extract':
                            next_postprocess_payload['force'] = False
                        
                        # only for export worker
                        if next_postprocess_type == 'export':
                            # company_batch_items for exporter
                            etl_company_batches_list = []
                            etl_company_batches_list = postgres_helpers.get_etl_company_batches_for_company_datasources(
                                company_datasource_ids=list(etl_case_study_data_dict.keys()),
                                logger=logger
                            )
                            next_postprocess_payload['company_batch_items'] = etl_company_batches_list

                        # send 1.0 progress payload to update into database
                        progress_payload = {
                            "type": postprocess_type,
                            "event": "progress",
                            "request_id": received_payload["request_id"],
                            "case_study_id": case_study_id,
                            "postprocess_id": etl_postprocess['postprocess_id'],
                            "progress": 1.0,
                        }
                        pubsub_util.publish(
                            gcp_product_id=config.GCP_PROJECT,
                            topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                            payload=progress_payload,
                            logger=logger
                        )
                            
                        # send to next step
                        next_etl_postprocess_topic = config.STEP_TYPE_TO_TOPICS[next_postprocess_type]
                        pubsub_util.publish(
                            gcp_product_id=config.GCP_PROJECT,
                            topic=next_etl_postprocess_topic,
                            payload=next_postprocess_payload,
                            logger=logger
                        )
                    
                    else:
                        # send 1.0 for the nlp_index
                        progress_payload = {
                            "type": postprocess_type,
                            "event": "progress",
                            "request_id": received_payload["request_id"],
                            "case_study_id": case_study_id,
                            "postprocess_id": etl_postprocess['postprocess_id'],
                            "progress": 1.0,
                        }
                        pubsub_util.publish(
                            gcp_product_id=config.GCP_PROJECT,
                            topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                            payload=progress_payload,
                            logger=logger
                        )
                        
                        # handle __FINISH__ step: wrap up case study
                        logger.info(f"[case_study_after_task_listener] case_study_id {case_study_id} is FINISHED!")
                        progress_payload = {
                            "type": config.CASE_STUDY_FINISHED_TYPE,
                            "event": "progress",
                            "request_id": received_payload["request_id"],
                            "case_study_id": case_study_id,
                            "postprocess_id": etl_postprocess['postprocess_id'],
                            "progress": 1.0,
                        }
                        pubsub_util.publish(
                            gcp_product_id=config.GCP_PROJECT,
                            topic=config.GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS,
                            payload=progress_payload,
                            logger=logger
                        )                        
                    
                    # acknowledge message
                    logger.info(f"[case_study_after_task_listener] postprocess {postprocess_id} - "
                                f"case_study_id {case_study_id} handled successfully! Acknowledging the message...")
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
                    )
                    
                except Exception as e:
                    logger.exception(f"[case_study_after_task_listener] error happens "
                                     f"while handling message {received_message}")


@auto_session
@auto_logger('case_study_progress_updater')
def case_study_progress_updater(session=None, logger=None):
    """
    listen to internal progress of ETLPostprocess steps on internal progress topic. 
    When receive a progress message:
    - update progress to appropriate ETLPostprocess if new progress is larger than current progress
    - calculate case study progress
        => update into etl_case_study
        => send to cs_progress topic for web-app
    """
    progress_sample_message = {
        "type": "postprocess_type",
        "event": "progress",
        "case_study_id": "case_study_id",
        "postprocess_id": "postprocess_id",
        "progress": 1.0,
    }
    
    logger.info("[case_study_progress_updater] starts listening for progress messages...")
    project_id = config.GCP_PROJECT
    cs_progress_subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_CS_INTERNAL_PROGRESS
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, cs_progress_subscription_id)
    
    with subscriber:
        while True:
            logger.info(f"[case_study_progress_updater] listening on {cs_progress_subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )
            
            if not response:
                time.sleep(config.SLEEP)
                continue
                
            for received_message in response.received_messages:
                logger.info(f"[case_study_progress_updater] received: {received_message.message.data}")
                received_payload = json.loads(received_message.message.data)
                
                try:
                    # parse message
                    postprocess_type = received_payload['type']
                    postprocess_id = received_payload['postprocess_id']
                    case_study_id = received_payload['case_study_id']
                    new_progress_value = received_payload['progress']

                    if postprocess_type == config.CASE_STUDY_FINISHED_TYPE:
                        logger.info(f"[case_study_progress_updater] case_study_id {case_study_id} is DONE, "
                                    f"publishing 100% progress...")
                        web_progress_payload = handlers.handle_progress_update_case_study_finish(
                            payload=received_payload,
                            logger=logger
                        )
                    else: # normal progress update    
                        logger.info(f"[case_study_progress_updater] case_study_id {case_study_id} - "
                                    f"{postprocess_type} progress update: {new_progress_value}")                        
                        web_progress_payload = handlers.handle_progress_update_case_study_postprocess_progress(
                            payload=received_payload,
                            logger=logger
                        )
                            
                    # send progress to web-app
                    pubsub_util.publish(
                        gcp_product_id=config.GCP_PROJECT,
                        topic=config.GCP_PUBSUB_TOPIC_CS_PROGRESS,
                        payload=web_progress_payload,
                        logger=logger
                    )
                    
                    # acknowledge message
                    logger.info(f"[case_study_progress_updater] case_study_id {case_study_id} - "
                                f"postprocess {postprocess_id} - {postprocess_type} "
                                "progress handled successfully! Acknowledging the message...")
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
                    )
                    time.sleep(5)
                    
                except Exception as e:
                    logger.exception(f"[case_study_progress_updater] unable to process received message!")
                
    
if __name__ == '__main__':
    listeners = {
        'data_consume_data_flow_listener': data_consume_data_flow_listener,
        'case_study_company_datasource_progress_updater': case_study_company_datasource_updater,
        'case_study_after_task_listener': case_study_after_task_listener,
        'case_study_progress_updater': case_study_progress_updater,
    }

    import argparse

    parser = argparse.ArgumentParser(
        description='Data consume orchestrator workers')
    parser.add_argument('--listener', help='Name of worker')
    args = parser.parse_args()
    runner = listeners.get(args.listener)
    if not runner:
        print(f"No listener found with name: {args.listener}")
    else:
        runner()
                    
