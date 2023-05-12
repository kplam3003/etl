import sys
import datetime as dt
import time
import json


sys.path.append('../')

from google.cloud import pubsub_v1
import pytz
from sqlalchemy import and_

import postgres_helpers
import config
import utils
import helpers
from database import (
    ETLRequest,
    ETLCompanyBatch,
    ETLStep,
    ETLStepDetail,
    ETLCompanyDatasourceCommon,
    auto_session
)
from core.logger import auto_logger
from core.operation import auto_killer, GracefulKiller

utc = pytz.UTC

logger = utils.load_logger()

NEXT_STEP_TYPES = {
    'crawl': 'preprocess',
    'preprocess': 'translate',
    'translate': 'load',
    'load': None,
}

STEP_TYPE_TO_TOPICS = {
    'crawl': config.GCP_PUBSUB_TOPIC_CRAWL,
    'translate': config.GCP_PUBSUB_TOPIC_TRANSLATE,
    'preprocess': config.GCP_PUBSUB_TOPIC_PREPROCESS,
    'load': config.GCP_PUBSUB_TOPIC_LOAD
}

EVENT_TO_STATUS = {
    'finish': 'finished',
    'fail': 'completed with error'
}


@auto_session
@auto_logger('listen_after_task')
@auto_killer
def listen_pubsub_after_task(session=None, logger=None, set_terminator=None):
    project_id = config.GCP_PROJECT
    subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_AFTER_TASK
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id)

    with subscriber:
        while True:
            logger.info(f"Listening on {subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )

            if not response:
                logger.info(f"No more task!!")
                time.sleep(config.SLEEP)
                continue

            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")

                try:
                    helpers.handle_after_task(received_message, session=session)
                    logger.info(f"Handle success, acknowledges the received message {received_message.message.data}")
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": [received_message.ack_id]}
                    )
                    payload = json.loads(received_message.message.data)
                    request_id = payload.get('request_id') or payload.get('step_detail').get('request_id')
                    if request_id:
                        etl_request = session.query(ETLRequest).get(request_id)
                        data_type = etl_request.data_type
                        etl_company_datasource = (
                            session
                            .query(ETLCompanyDatasourceCommon)
                            .filter(
                                and_(
                                    ETLCompanyDatasourceCommon.request_id == request_id,
                                    ETLCompanyDatasourceCommon.data_type == data_type
                                )
                            )
                            .first()
                        )

                        if etl_company_datasource:
                            helpers.auto_update_etl_company_datasource_progress(
                                etl_company_datasource=etl_company_datasource,
                                session=session
                            )
                        helpers.publish_company_datasource_progress(session=session, etl_request=etl_request)
                except:
                    logger.exception(f"[ERROR] Unable to process {received_message}")


@auto_session
@auto_logger('listen_progress')
def listen_pubsub_progress(session=None, logger=None):
    project_id = config.GCP_PROJECT
    subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_PROGRESS
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id)

    with subscriber:
        while True:
            logger.info(f"Listening on {subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )

            if not response:
                time.sleep(config.SLEEP)
                continue

            ack_ids = []
            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")
                ack_ids.append(received_message.ack_id)

            logger.info(f"Acknowledges the received message...")
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

            data = json.loads(received_message.message.data)
            publish_time = received_message.message.publish_time
            publish_time = publish_time.astimezone()

            event = data.get('event')
            progress = data.get('progress')

            step_type = data.get('type')
            batch_id = data.get('batch').get('batch_id')
            step_id = data.get('step').get('step_id')
            step_detail_id = data.get('step_detail').get('step_detail_id')

            if not event or not step_type or not batch_id or not step_id or not step_detail_id:
                logger.info(f"Invalid payload")
                continue
            # 1. Update current task progress
            logger.info(
                f"Updating step detail {step_detail_id} progress: {progress}")

            step_detail = None
            try:
                step_detail = session.query(ETLStepDetail).filter(
                    ETLStepDetail.step_detail_id == step_detail_id).with_for_update().one()
            except:
                session.rollback()

            if not step_detail:
                logger.info(f"Unable to find step detail {step_detail_id}")
                continue

            updated_time = step_detail.updated_at.astimezone()
            if updated_time >= publish_time:
                logger.info(
                    f"Step detail {step_detail_id} newest than current progress. {updated_time.isoformat()} >= {publish_time.isoformat()}")
                session.commit()
                continue

            step_detail.status = 'running' if step_detail.status == 'waiting' else step_detail.status
            step_detail.updated_at = publish_time
            step_detail.progress_current = progress.get('current')
            step_detail.progress_total = progress.get('total')

            this_step = session.query(ETLStep).get(step_id)
            this_batch = session.query(ETLCompanyBatch).get(batch_id)

            this_step.status = 'running'
            this_step.updated_at = publish_time

            this_batch.status = 'running'
            this_batch.updated_at = publish_time

            session.commit()

            this_request = session.query(ETLRequest).get(this_batch.request_id)
            data_type = this_request.data_type

            if this_request and this_request.status not in ['finished', 'completed with error']:
                company_datasource_id = data.get('batch', {}).get("company_datasource_id")
                if company_datasource_id:
                    etl_company_datasource = (
                        session
                        .query(ETLCompanyDatasourceCommon)
                        .filter(
                            and_(
                                ETLCompanyDatasourceCommon.company_datasource_id == company_datasource_id,
                                ETLCompanyDatasourceCommon.data_type == data_type
                            )
                        )
                        .first()
                    )
                    helpers.auto_update_etl_company_datasource_progress(
                        etl_company_datasource=etl_company_datasource, session=session
                    )

                helpers.publish_company_datasource_progress(session=session, etl_request=this_request)


@auto_session
@auto_logger('auto_updater')
def status_updater(session=None, logger=None, run_once=False):
    logger.info("[company_datasource_progress_updater] starts listening for progress messages...")
    project_id = config.GCP_PROJECT
    publisher = pubsub_v1.PublisherClient()

    while True:
        logger.info(f"finding unfinished request...")
        requests = session.query(ETLRequest).filter(
            ETLRequest.status.notin_(('finished', 'completed with error')),
            ETLRequest.request_type == "company_datasource"
        ).all()
        if not requests:
            logger.info(f"No unfinished request found. Everything is good!!")
        else:
            logger.info(f"Found {len(requests)} unfinished requests {list(map(lambda r: r.request_id, requests))}")
            for request in requests:
                etl_batches = session.query(ETLCompanyBatch).filter(
                    ETLCompanyBatch.request_id == request.request_id).all()
                etl_steps = session.query(ETLStep).filter(ETLStep.request_id == request.request_id).all()

                logger.info(f"Updating etl steps status...")
                for step in etl_steps:
                    helpers.update_step_status(session=session, etl_step=step)

                logger.info(f"Updating etl batches status...")
                for batch in etl_batches:
                    helpers.update_batch_status(session=session, etl_batch=batch)
                logger.info(f"Updating etl requests status...")
                helpers.update_request_and_company_datasource_status(session=session, etl_request=request)
                if request.status in ['finished', 'completed with error']:
                    logger.info(f"Request {request.request_id} finished. Publishing response to Pub/Sub...")
                    # Publish current message to responses pubsub topic
                    helpers.publish_company_datasource_progress(session=session, etl_request=request)

                    if request.status == 'completed with error':
                        # Check whether all step detail have data to process, otherwise publish fail error
                        etl_details = session.query(ETLStepDetail) \
                            .filter(ETLStepDetail.request_id == request.request_id) \
                            .filter(ETLStepDetail.status == 'finished').all()

                        failed = False
                        message = ""
                        if len(etl_details) == 0:
                            failed = True
                            message = "No finished step details found"
                        else:
                            total_processed_reviews = sum(list(map(lambda x: x.item_count or 0, etl_details)))
                            if total_processed_reviews == 0:
                                failed = True
                                message = "Total processed reviews is 0"

                        if failed:
                            etl_company_datasource = postgres_helpers.get_etl_company_datasource_by_etl_request_id(
                                etl_request_id=request.request_id,
                                data_type=request.data_type,
                                logger=logger
                            )
                            logger.info(
                                f"[ERROR] The request_id: {request.request_id} - company_datasource_id: {etl_company_datasource.company_datasource_id} FAIL completely.")
                            payload = json.dumps({
                                'event': 'error',
                                'company_datasource_id': etl_company_datasource.company_datasource_id,
                                'error': message,
                                'error_code': 'FAILED',
                                'data_type': request.data_type
                            })
                            payload = payload.encode('utf-8')
                            topic_id = config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR
                            topic_path = publisher.topic_path(project_id, topic_id)
                            logger.info(f"Publishing a message to {topic_id}: {payload}")
                            future = publisher.publish(topic_path, payload)
                            logger.info(f"Published message to {topic_id}, result: {future.result()}")
                else:
                    logger.info(f"Publishing case study progress...")
                    helpers.publish_company_datasource_progress(session=session, etl_request=request)
        # Exit if run only once
        if run_once:
            break

        time.sleep(config.SLEEP)


@auto_session
@auto_logger('data_flow')
def data_flow(session=None, logger=None, run_once=False):
    project_id = config.GCP_PROJECT
    subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_DATA_FLOW
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_id, subscription_id)

    with subscriber:
        while True:
            logger.info(f"Listening on {subscription_id}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )

            if not response:
                time.sleep(config.SLEEP)
                continue

            ack_ids = []
            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")
                ack_ids.append(received_message.ack_id)

            data = json.loads(received_message.message.data)
            event = data.get('event')
            request_id = data.get('request_id')
            request = session.query(ETLRequest).get(request_id)

            logger.info(f"Acknowledges the received message...")
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )


if __name__ == '__main__':
    listeners = {
        'after_task': listen_pubsub_after_task,
        'progress': listen_pubsub_progress,
        'status_updater': status_updater,
        'data_flow': data_flow
    }

    import argparse

    parser = argparse.ArgumentParser(
        description='Listen to a PubSub subscription.')
    parser.add_argument('--listener', help='Name of the listener')
    args = parser.parse_args()
    runner = listeners.get(args.listener)
    if not runner:
        print(f"No listener found with name: {args.listener}")
    else:
        runner()
