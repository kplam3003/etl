import datetime as dt
import json
import uuid

import numpy as np
import pandas as pd
from google.cloud import pubsub_v1
from googleapiclient.discovery import build
from sqlalchemy import func, or_, and_

import config
import postgres_helpers
from core.logger import auto_logger
from core.error_util import publish_error_message
from database import (
    ETLRequest, 
    ETLCompanyBatch, 
    ETLStep, 
    ETLStepDetail,
    auto_session,
    ETLCompanyDatasourceCommon,
)

NEXT_STEP_TYPES = {
    'crawl': 'preprocess',
    'preprocess': 'translate',
    'translate': 'load',
    'load': None
}

STEP_TYPE_TO_TOPICS = {
    'crawl': config.GCP_PUBSUB_TOPIC_CRAWL,
    'translate': config.GCP_PUBSUB_TOPIC_TRANSLATE,
    'preprocess': config.GCP_PUBSUB_TOPIC_PREPROCESS,
    'load': config.GCP_PUBSUB_TOPIC_LOAD,
}

EVENT_TO_STATUS = {
    'finish': 'finished',
    'fail': 'completed with error'
}


@auto_session
@auto_logger
def find_batches(status=None, session=None, logger=None):
    query = session.query(ETLCompanyBatch)
    if status:
        query = query.filter(ETLCompanyBatch.status == status)

    batches = query.all()
    return batches


@auto_session
@auto_logger
def update_batch(batch_id=None, status=None, session=None, logger=None):
    assert batch_id is not None, "Need batch id to lookup"

    instance = session.query(ETLCompanyBatch).get(batch_id)
    instance.status = status
    instance.updated_at = dt.datetime.today()

    session.commit()
    return instance


@auto_logger
def generate_startup_script(logger=None):
    scripts = f"""
#! /bin/bash

mkdir /tmp/dispatcher
mkdir /tmp/output
mkdir /tmp/crawler

cat <<EOF > /tmp/crawler/docker-compose.yml
version: '3.1'

services:
    sqlproxy:
        image: gcr.io/cloudsql-docker/gce-proxy:1.16
        restart: always
        command: /cloud_sql_proxy -instances={config.GCP_PROJECT}:{config.GCP_DB_INSTANCE}=tcp:0.0.0.0:5432

    dispatcher:
        image: gcr.io/{config.GCP_PROJECT}/leo-crawler-dispatcher:{config.ENV}
        restart: always
        environment:
            LOGGER_NAME: {config.ENV}_dispatcher
            DATABASE_URI: {config.CRAWLER_DATABASE_URI}
            ROWS_PER_STEP: {config.ROWS_PER_STEP}
            WEB_WRAPPER_URL: {config.WEB_WRAPPER_URL}
            GCP_PROJECT: {config.GCP_PROJECT}
            GCP_PUBSUB_SUBSCRIPTION_DISPATCHER: {config.GCP_PUBSUB_SUBSCRIPTION_DISPATCHER}
            GCP_PUBSUB_TOPIC_DISPATCH: {config.GCP_PUBSUB_TOPIC_DISPATCH}
            GCP_PUBSUB_TOPIC_CRAWL: {config.GCP_PUBSUB_TOPIC_CRAWL}
            GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR: {config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR}
            GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR: {config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR}
            DATASHAKE_API: {config.DATASHAKE_API}
            DATASHAKE_TOKEN: {config.DATASHAKE_TOKEN}
            LUMINATI_HTTP_PROXY: {config.LUMINATI_HTTP_PROXY}
            WEBHOOK_API: {config.WEBHOOK_API}
            WEBHOOK_TOKEN: {config.WEBHOOK_TOKEN}
            REDDIT_CLIENT_ID: {config.REDDIT_CLIENT_ID}
            REDDIT_CLIENT_SECRET: {config.REDDIT_CLIENT_SECRET}
            ENABLE_CLOUD_LOGGING: 1
            LUMINATI_WEBUNBLOCKER_HTTP_PROXY: {config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY}
            CORESIGNAL_JWT: {config.CORESIGNAL_JWT}
            MONGODB_DATABASE_URI: {config.MONGODB_DATABASE_URI}
            MONGODB_DATABASE_NAME: {config.MONGODB_DATABASE_NAME}
            MONGODB_DATABASE_ROOT_CA: {config.MONGODB_DATABASE_ROOT_CA}
            MONGODB_DATABASE_KEY_FILE: {config.MONGODB_DATABASE_KEY_FILE}
            MONGODB_VOC_REVIEW_COLLECTION: {config.MONGODB_VOC_REVIEW_COLLECTION}
            MONGODB_VOE_REVIEW_COLLECTION: {config.MONGODB_VOE_REVIEW_COLLECTION}
            MONGODB_VOE_OVERVIEW_COLLECTION: {config.MONGODB_VOE_OVERVIEW_COLLECTION}
            MONGODB_VOE_JOB_COLLECTION: {config.MONGODB_VOE_JOB_COLLECTION}
            MONGODB_VOC_REVIEW_STATS: {config.MONGODB_VOC_REVIEW_STATS}
            MONGODB_VOE_REVIEW_STATS: {config.MONGODB_VOE_REVIEW_STATS}
            MONGODB_CORESIGNAL_STATS: {config.MONGODB_CORESIGNAL_STATS}
            MONGODB_CORESIGNAL_EMPLOYEES: {config.MONGODB_CORESIGNAL_EMPLOYEES}
            MONGODB_CORESIGNAL_CD_MAPPING: {config.MONGODB_CORESIGNAL_CD_MAPPING}
            GCP_BQ_TABLE_VOE_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS}
            GCP_BQ_TABLE_VOC_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS}
            GCP_BQ_TABLE_HRA_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS}

        entrypoint: python subscribers.py --listener listen_new_batches
        volumes:
            - /tmp/dispatcher:/tmp/dispatcher

    crawler01:
        image: gcr.io/{config.GCP_PROJECT}/leo-scalable-crawler:{config.ENV}
        restart: always
        environment:
            LOGGER_NAME: {config.ENV}_crawler
            TASKS_DIR: /tmp/dispatcher
            DATABASE_URI: {config.CRAWLER_DATABASE_URI}
            CRAWLER_ID: crawler01
            OUTPUT_DIR: /tmp/output
            WEB_WRAPPER_URL: {config.WEB_WRAPPER_URL}
            ROWS_PER_STEP: {config.ROWS_PER_STEP}
            GCP_PROJECT_ID: {config.GCP_PROJECT}
            GCP_STORAGE_BUCKET: {config.GCP_STORAGE_BUCKET}
            GCP_PUBSUB_TOPIC_PROGRESS: {config.GCP_PUBSUB_TOPIC_PROGRESS}
            GCP_PUBSUB_TOPIC_AFTER_TASK: {config.GCP_PUBSUB_TOPIC_AFTER_TASK}
            GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR: {config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR}
            GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR: {config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR}
            GCP_PUBSUB_TOPIC_CRAWL: {config.GCP_PUBSUB_TOPIC_CRAWL}
            GCP_PUBSUB_SUBSCRIPTION_CRAWLERS: {config.GCP_PUBSUB_SUBSCRIPTION_CRAWLERS}
            DATASHAKE_API: {config.DATASHAKE_API}
            DATASHAKE_TOKEN: {config.DATASHAKE_TOKEN}
            LUMINATI_HTTP_PROXY: {config.LUMINATI_HTTP_PROXY}
            WEBHOOK_API: {config.WEBHOOK_API}
            WEBHOOK_TOKEN: {config.WEBHOOK_TOKEN}
            REDDIT_CLIENT_ID: {config.REDDIT_CLIENT_ID}
            REDDIT_CLIENT_SECRET: {config.REDDIT_CLIENT_SECRET}
            ENABLE_CLOUD_LOGGING: 1
            LUMINATI_WEBUNBLOCKER_HTTP_PROXY: {config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY}
            MONGODB_DATABASE_URI: {config.MONGODB_DATABASE_URI}
            MONGODB_DATABASE_NAME: {config.MONGODB_DATABASE_NAME}
            CORESIGNAL_JWT: {config.CORESIGNAL_JWT}
            GCP_BQ_TABLE_VOE_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS}
            GCP_BQ_TABLE_VOC_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS}
            GCP_BQ_TABLE_HRA_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS}
        entrypoint: python subscribers.py --listener listen_crawl_task
        volumes:
            - /tmp/dispatcher:/tmp/dispatcher
            - /tmp/output:/tmp/output

    crawler02:
        image: gcr.io/{config.GCP_PROJECT}/leo-scalable-crawler:{config.ENV}
        restart: always
        environment:
            LOGGER_NAME: {config.ENV}_crawler
            TASKS_DIR: /tmp/dispatcher
            DATABASE_URI: {config.CRAWLER_DATABASE_URI}
            CRAWLER_ID: crawler02
            OUTPUT_DIR: /tmp/output
            WEB_WRAPPER_URL: {config.WEB_WRAPPER_URL}
            ROWS_PER_STEP: {config.ROWS_PER_STEP}
            GCP_PROJECT_ID: {config.GCP_PROJECT}
            GCP_STORAGE_BUCKET: {config.GCP_STORAGE_BUCKET}
            GCP_PUBSUB_TOPIC_PROGRESS: {config.GCP_PUBSUB_TOPIC_PROGRESS}
            GCP_PUBSUB_TOPIC_AFTER_TASK: {config.GCP_PUBSUB_TOPIC_AFTER_TASK}
            GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR: {config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR}
            GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR: {config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR}
            GCP_PUBSUB_TOPIC_CRAWL: {config.GCP_PUBSUB_TOPIC_CRAWL}
            GCP_PUBSUB_SUBSCRIPTION_CRAWLERS: {config.GCP_PUBSUB_SUBSCRIPTION_CRAWLERS}
            DATASHAKE_API: {config.DATASHAKE_API}
            DATASHAKE_TOKEN: {config.DATASHAKE_TOKEN}
            LUMINATI_HTTP_PROXY: {config.LUMINATI_HTTP_PROXY}
            WEBHOOK_API: {config.WEBHOOK_API}
            WEBHOOK_TOKEN: {config.WEBHOOK_TOKEN}
            REDDIT_CLIENT_ID: {config.REDDIT_CLIENT_ID}
            REDDIT_CLIENT_SECRET: {config.REDDIT_CLIENT_SECRET}
            ENABLE_CLOUD_LOGGING: 1
            LUMINATI_WEBUNBLOCKER_HTTP_PROXY: {config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY}
            MONGODB_DATABASE_URI: {config.MONGODB_DATABASE_URI}
            MONGODB_DATABASE_NAME: {config.MONGODB_DATABASE_NAME}
            CORESIGNAL_JWT: {config.CORESIGNAL_JWT}
            GCP_BQ_TABLE_VOE_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS}
            GCP_BQ_TABLE_VOC_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS}
            GCP_BQ_TABLE_HRA_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS}
        entrypoint: python subscribers.py --listener listen_crawl_task
        volumes:
            - /tmp/dispatcher:/tmp/dispatcher
            - /tmp/output:/tmp/output

    crawler03:
        image: gcr.io/{config.GCP_PROJECT}/leo-scalable-crawler:{config.ENV}
        restart: always
        environment:
            LOGGER_NAME: {config.ENV}_crawler
            TASKS_DIR: /tmp/dispatcher
            DATABASE_URI: {config.CRAWLER_DATABASE_URI}
            CRAWLER_ID: crawler03
            OUTPUT_DIR: /tmp/output
            WEB_WRAPPER_URL: {config.WEB_WRAPPER_URL}
            ROWS_PER_STEP: {config.ROWS_PER_STEP}
            GCP_PROJECT_ID: {config.GCP_PROJECT}
            GCP_STORAGE_BUCKET: {config.GCP_STORAGE_BUCKET}
            GCP_PUBSUB_TOPIC_PROGRESS: {config.GCP_PUBSUB_TOPIC_PROGRESS}
            GCP_PUBSUB_TOPIC_AFTER_TASK: {config.GCP_PUBSUB_TOPIC_AFTER_TASK}
            GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR: {config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR}
            GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR: {config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR}
            GCP_PUBSUB_TOPIC_CRAWL: {config.GCP_PUBSUB_TOPIC_CRAWL}
            GCP_PUBSUB_SUBSCRIPTION_CRAWLERS: {config.GCP_PUBSUB_SUBSCRIPTION_CRAWLERS}
            DATASHAKE_API: {config.DATASHAKE_API}
            DATASHAKE_TOKEN: {config.DATASHAKE_TOKEN}
            LUMINATI_HTTP_PROXY: {config.LUMINATI_HTTP_PROXY}
            WEBHOOK_API: {config.WEBHOOK_API}
            WEBHOOK_TOKEN: {config.WEBHOOK_TOKEN}
            REDDIT_CLIENT_ID: {config.REDDIT_CLIENT_ID}
            REDDIT_CLIENT_SECRET: {config.REDDIT_CLIENT_SECRET}
            ENABLE_CLOUD_LOGGING: 1
            LUMINATI_WEBUNBLOCKER_HTTP_PROXY: {config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY}
            MONGODB_DATABASE_URI: {config.MONGODB_DATABASE_URI}
            MONGODB_DATABASE_NAME: {config.MONGODB_DATABASE_NAME}
            CORESIGNAL_JWT: {config.CORESIGNAL_JWT}
            GCP_BQ_TABLE_VOE_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS}
            GCP_BQ_TABLE_VOC_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS}
            GCP_BQ_TABLE_HRA_CRAWL_STATISTICS: {config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS}
        entrypoint: python subscribers.py --listener listen_crawl_task
        volumes:
            - /tmp/dispatcher:/tmp/dispatcher
            - /tmp/output:/tmp/output

EOF

cd /tmp/crawler
gcloud auth configure-docker --quiet

docker-compose up -d
docker ps

    """

    return scripts


@auto_logger
def list_vm(step_type='crawl', logger=None):
    compute = build('compute', 'beta')
    instances = compute.instances()
    instances = compute.instances()
    response = instances.list(
        project=config.GCP_PROJECT,
        zone=config.GCP_ZONE,
        filter=f'labels.type={step_type} AND status=RUNNING'
    ).execute()
    return response


@auto_logger
def create_vm(step_type="crawl", logger=None):
    compute = build('compute', 'beta')
    instances = compute.instances()
    scripts = generate_startup_script()
    with open('/tmp/crawler_startup.sh', 'w+') as f:
        f.write(scripts)

    startup_script = open('/tmp/crawler_startup.sh').read()

    response = instances.insert(
        project=config.GCP_PROJECT,
        zone=config.GCP_ZONE,
        body={
            'sourceMachineImage': f'global/machineImages/{config.GCP_MACHINE_IMAGE}',
            'name': f"{step_type}-{uuid.uuid4()}",
            'metadata': {
                'items': [{
                    'key': 'startup-script',
                    'value': startup_script
                }]
            },
            'labels': {
                'type': step_type,
            }
        }
    ).execute()

    return response


@auto_logger
def delete_vm(vm_name, step_type="crawl", logger=None):
    compute = build('compute', 'beta')
    instances = compute.instances()
    response = instances.delete(
        project=config.GCP_PROJECT,
        zone=config.GCP_ZONE,
        instance=vm_name
    ).execute()

    return response


@auto_logger
def stop_vm(batch: ETLCompanyBatch, step_type="crawl", logger=None):
    compute = build('compute', 'beta')
    instances = compute.instances()
    response = instances.stop(
        project=config.GCP_PROJECT,
        zone=config.GCP_ZONE,
        instance=f"{step_type}-{batch.batch_name}"
    ).execute()

    return response


@auto_session
@auto_logger
def create_step(batch: ETLCompanyBatch, step_type="crawl", session=None, logger=None):
    logger.info(
        f"Creating step type {step_type} for batch {batch.batch_name} - Check for step type existing...")

    etl_step = session.query(ETLStep).filter(
        ETLStep.batch_id == batch.batch_id).filter(ETLStep.step_type == step_type).first()
    if etl_step:
        logger.info(
            f"Found existing {step_type} step type. step_id {etl_step.step_id} - name: {etl_step.step_name}")
        return etl_step

    logger.info(f"No existing step found. Creating...")
    today = dt.datetime.now()
    etl_step = ETLStep(
        step_name=f"{step_type}-{batch.batch_name}",
        batch_id=batch.batch_id,
        created_at=today,
        updated_at=today,
        request_id=batch.request_id,
        status="waiting",
        step_type=step_type
    )
    session.add(etl_step)
    session.commit()

    logger.info(f"Step type {step_type} created. step_id: {etl_step.step_id}")
    return etl_step


@auto_session
@auto_logger
def update_step_status(session=None, etl_step=None, logger=None):
    logger.info(f"Updating step {etl_step.step_id} status...")
    etl_details = session.query(ETLStepDetail).filter(
        ETLStepDetail.step_id == etl_step.step_id).all()
    if not etl_details:
        logger.info(f"No step detail found")
        return

    statuses = list(map(lambda it: it.status, etl_details))
    status = etl_step.status
    if all(list(map(lambda s: s == 'finished', statuses))):
        status = 'finished'
    elif any(list(map(lambda s: s == 'running', statuses))):
        status = 'running'
    elif any(list(map(lambda s: s == 'completed with error', statuses))):
        status = 'completed with error'

    etl_step.status = status
    etl_step.updated_at = dt.datetime.today()
    session.commit()


@auto_session
@auto_logger
def update_batch_status(session=None, etl_batch=None, logger=None):
    logger.info(f"Update batch {etl_batch.batch_id} status")
    etl_steps = session.query(ETLStep).filter(
        ETLStep.batch_id == etl_batch.batch_id).all()
    if not etl_steps:
        logger.info(f"No step found")
        return

    statuses = list(map(lambda it: it.status, etl_steps))
    status = etl_batch.status
    if all(list(map(lambda s: s == 'finished', statuses))):
        status = 'finished'
    elif any(list(map(lambda s: s == 'running', statuses))):
        status = 'running'
    elif any(list(map(lambda s: s == 'completed with error', statuses))):
        status = 'completed with error'

    etl_batch.status = status
    etl_batch.updated_at = dt.datetime.today()
    session.commit()


@auto_session
@auto_logger
def update_request_and_company_datasource_status(
    session=None, etl_request=None, logger=None
):
    """
    Update status of request and company datasource.
    These 2 need to be updated together, that's why they are in the same function
    """
    logger.info(f"Update request {etl_request.request_id} status")
    etl_batches = session.query(ETLCompanyBatch).filter(
        ETLCompanyBatch.request_id == etl_request.request_id).all()
    if not etl_batches:
        logger.info(f"No batches found")
        return

    statuses = list(map(lambda it: it.status, etl_batches))
    current_status = etl_request.status
    new_status = None
    if all(list(map(lambda s: s == 'finished', statuses))):
        new_status = 'finished'
    elif any(list(map(lambda s: s == 'running', statuses))):
        new_status = 'running'
    elif any(list(map(lambda s: s == 'completed with error', statuses))):
        new_status = 'completed with error'

    if new_status == current_status or new_status is None:
        logger.info(
            f'Request_id {etl_request.request_id}, status {etl_request.status} '
            'does not need to be updated'
        )
        return
    
    else:
        etl_request.status = new_status
        etl_request.updated_at = dt.datetime.today()
        session.commit()

    # Update company data source is running with this request
    # each kind of data_type (job, review, review_stats, or all, all is legacy) 
    # has its own table
    request_id = etl_request.request_id
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
    ) if ETLCompanyDatasourceCommon else None

    if not etl_company_datasource:
        logger.warning(
            f"No etl_company_datasource found "
            f"for request_id {request_id} "
            f"and data_type {data_type}"
        )
        return

    logger.info(f"request_id {request_id} status updated to {new_status}")
    logger.info(
        f"company_datasource_id {etl_company_datasource.company_datasource_id} "
        f"status updated to {new_status}"
    )
    etl_company_datasource.status = new_status
    etl_company_datasource.updated_at = dt.datetime.today()

    session.commit()


@auto_session
@auto_logger
def update_running_batches_status(session=None, logger=None):
    running_batches = session.query(ETLCompanyBatch).filter(
        ETLCompanyBatch.status == 'running').all()
    if not running_batches:
        logger.info(f"There are no running batch to check for")
        return

    logger.info(f"Updating status for {len(running_batches)} batches")
    for batch in running_batches:
        etl_steps = session.query(ETLStep).filter(
            ETLStep.batch_id == batch.batch_id).all()

        status = 'finished'  # Assume status is finished
        for etl_step in etl_steps:
            if etl_step.status in ['waiting', 'running']:
                status = 'running'
                break

            if etl_step.status == 'completed with error':
                status = 'completed with error'

        batch.status = status
        batch.updated_at = dt.datetime.today()
        session.commit()


@auto_session
@auto_logger
def update_running_requests_status(session=None, logger=None):
    running_requests = session.query(ETLRequest).filter(
        ETLRequest.status == 'running').all()
    if not running_requests:
        logger.info(f"There are no running requests to update status")
        return

    logger.info(f"Updating status for {len(running_requests)} batches")
    for request in running_requests:
        etl_batches = session.query(ETLCompanyBatch).filter(
            ETLCompanyBatch.request_id == request.request_id).all()

        status = 'finished'  # Assume status is finished
        for batch in etl_batches:
            if batch.status in ['waiting', 'running']:
                status = 'running'
                break

            if batch.status == 'completed with error':
                status = 'completed with error'

        request.status = status
        request.updated_at = dt.datetime.today()
        session.commit()



@auto_session
@auto_logger
def clean_etl_steps_vm(session=None, logger=None):
    # Clean crawl vm
    response = list_vm(step_type='crawl')
    crawl_vms = response.get('items') or []
    logger.info(f"clean_etl_steps_vm: found running vms {len(crawl_vms)}")
    if crawl_vms:
        for vm in crawl_vms:
            vm_name = vm.get('name')
            vm_labels = vm.get("labels")
            step_id = (vm_labels or {}).get('step_id')

            logger.info(
                f"Checking batch status on this vm step_id: {step_id}, name: {vm_name}, labels: {vm_labels}")

            batch = None
            etl_step = None

            if step_id:
                etl_step = session.query(ETLStep).get(int(step_id))
                batch = session.query(ETLCompanyBatch).get(etl_step.batch_id)
            else:
                batch_name = vm_name.replace('crawl-', '')
                batch = session.query(ETLCompanyBatch).filter(
                    ETLCompanyBatch.batch_name == batch_name).first()
                assert batch is not None, 'Must have link a vm with a batch'
                logger.info(
                    f"Batch link with this vm had batch_id: {batch.batch_id}")

                etl_step = session.query(ETLStep).filter(
                    ETLStep.batch_id == batch.batch_id).filter(ETLStep.step_type == 'crawl').first()

            assert etl_step is not None, 'Must have link a vm with a etl step'
            logger.info(
                f"ETLStep link with this vm had step_id: {etl_step.step_id}")

            if etl_step.status == 'finished':
                logger.info(
                    f"step {etl_step.step_id} is finished, deleting vm...")
                delete_vm(batch, step_type='crawl')
                logger.info(f"VM {vm_name} is deleted")
            elif etl_step.status == 'completed with error':
                logger.info(
                    f"Step {etl_step.step_id} is error, stopping the vm...")
                stop_vm(batch, step_type='crawl')
                logger.info(f"VM {vm_name} is stopped")
            else:
                logger.info(f"Step {etl_step.step_id} is still running")
    else:
        logger.info("No running vm to clean. Have fun")

    # Clean translate vm
    # TODO


@auto_session
@auto_logger
def check_running_vms_vs_running_steps(session=None, logger=None):
    logger.info(f"Looking for waiting step that have no VMs attached ...")
    etl_steps = session.query(ETLStep).filter(
        ETLStep.status == 'waiting').all()

    if not etl_steps:
        logger.info(f"Hmm, no running steps, all good.")
        return

    for step in etl_steps:
        batch = session.query(ETLCompanyBatch).get(step.batch_id)
        vm_name = f"{step.step_type}-{batch.batch_name}"

        response = list_vm(step_type=step.step_type)
        items = response.get('items') or []
        items = list(map(lambda it: it.get('name'), items))
        if not items or vm_name not in items:
            logger.info(
                f"Found step {step.step_id} that have no vm. Retring to start other vm...")
            if step.step_type != 'crawl':
                logger.info(
                    f"Unsupported step: {step.step_id}, {step.step_type}")
                continue

            create_vm(batch, step_type=step.step_type, step_id=step.step_id)
            logger.info(f"VMs {vm_name} recreated")


@auto_session
@auto_logger
def calculate_company_datasource_progress(logger=None, session=None, etl_request=None):
    request_id = etl_request.request_id
    NUM_STEPS = 4
    etl_batches = session.query(ETLCompanyBatch.batch_id).filter(ETLCompanyBatch.request_id == request_id).all()
    query = session.query(ETLCompanyBatch.batch_id, func.count(ETLStepDetail.step_detail_id)) \
        .outerjoin(ETLStep, ETLCompanyBatch.batch_id == ETLStep.batch_id) \
        .outerjoin(ETLStepDetail, ETLStep.step_id == ETLStepDetail.step_id) \
        .group_by(ETLCompanyBatch.batch_id)

    total_crawl_query = query \
        .filter(ETLCompanyBatch.request_id == request_id) \
        .filter(ETLStep.step_type == 'crawl').all()

    total_finished_query = query \
        .filter(ETLCompanyBatch.request_id == request_id) \
        .filter(or_(ETLStepDetail.status == 'finished', ETLStepDetail.status == 'completed with error')) \
        .group_by(ETLCompanyBatch.batch_id).all()

    progress_totals = []
    for batch_id, *_ in etl_batches:
        batch_crawls = list(filter(lambda it: it[0] == batch_id, total_crawl_query))
        crawl_count = batch_crawls[0][1] if batch_crawls else 0

        batch_finished_items = list(filter(lambda it: it[0] == batch_id, total_finished_query))
        finished_count = batch_finished_items[0][1] if batch_finished_items else 0

        total_count = crawl_count * NUM_STEPS
        progress = finished_count / total_count if total_count else 0.0

        progress_totals.append((batch_id, finished_count, total_count, progress))

    progress = sum(map(lambda it: it[-1], progress_totals)) / len(etl_batches) if etl_batches else 0.0

    return min(progress, 0.9999)


@auto_session
@auto_logger
def calculate_company_datasource_progress_details(logger=None, session=None, etl_request=None):
    logger.info(f"Calculating progress detail for request_id: {etl_request.request_id}")

    etl_batches = session.query(ETLCompanyBatch) \
        .filter(ETLCompanyBatch.request_id == etl_request.request_id) \
        .all()

    crawl_progress_slots = []
    preprocess_progress_slots = []
    translate_progress_slots = []
    load_progress_slots = []

    for batch in etl_batches:
        if batch.status in ['finished', 'completed with error']:
            crawl_progress_slots.append(1.0)
            preprocess_progress_slots.append(1.0)
            translate_progress_slots.append(1.0)
            load_progress_slots.append(1.0)
            continue

        query = session.query(ETLStepDetail
                              ).filter(ETLStepDetail.batch_id == batch.batch_id)
        df = pd.read_sql(query.statement, query.session.bind)
        df.loc[:, 'progress'] = df['progress_current'] / df['progress_total']
        df.loc[:, 'progress'] = df['progress'].fillna(0.0)
        df.loc[df['status'].isin(['finished', 'completed with error']), 'progress'] = 1.0

        # 1. Calculate crawl
        crawl_df = df[df['step_detail_name'].str.contains('crawl')]
        crawl_progress = np.mean(crawl_df['progress'].array or [0.0])
        crawl_progress_slots.append(crawl_progress)
        crawl_step_count = crawl_df.shape[0] or 1

        # 2. Calculate preprocess
        preprocess_df = df[df['step_detail_name'].str.contains('preprocess')]
        preprocess_progress = np.sum(preprocess_df['progress'].array or [0.0]) / crawl_step_count
        preprocess_progress_slots.append(preprocess_progress)

        # 3. Calculate translate
        translate_df = df[df['step_detail_name'].str.contains('translate')]
        translate_progress = np.sum(translate_df['progress'].array or [0.0]) / crawl_step_count
        translate_progress_slots.append(translate_progress)

        # 4. Calculate load
        load_df = df[df['step_detail_name'].str.contains('load')]
        load_progress = np.sum(load_df['progress'].array or [0.0]) / crawl_step_count
        load_progress_slots.append(load_progress)


    # In case there is not batches, it mean etl flow skip and reuse other etl data. So 1.0 in this case
    total_crawl = np.mean(crawl_progress_slots or [0.0])
    total_preprocess = np.mean(preprocess_progress_slots or [0.0])
    total_translate = np.mean(translate_progress_slots or [0.0])
    total_load = np.mean(load_progress_slots or [0.0])

    return {
        'total': np.mean([
            total_crawl,
            total_preprocess,
            total_translate,
            total_load,
        ]),

        'details': {
            'crawl': total_crawl,
            'preprocess': total_preprocess,
            'translate': total_translate,
            'load': total_load,
        }
    }


@auto_session
@auto_logger
def auto_update_etl_company_datasource_progress(etl_company_datasource, logger=None, session=None):
    etl_request = session.query(ETLRequest).get(etl_company_datasource.request_id)
    progress_info = calculate_company_datasource_progress_details(
        etl_request=etl_request,
        logger=logger, 
        session=session
    )

    etl_company_datasource.progress = progress_info["total"]

    session.commit()


@auto_session
@auto_logger
def publish_company_datasource_progress(logger=None, session=None, etl_request=None):
    data_type = etl_request.data_type
    company_datasource = postgres_helpers.get_etl_company_datasource_by_etl_request_id(
        etl_request.request_id,
        data_type=data_type,
        session=session
    )
    
    if not company_datasource:
        logger.warning(
            f"Company datasource not found "
            f"for request_id: {etl_request.request_id} "
            f"and data_type {data_type}"    
        )
        return

    logger.info(f"Calculating progress info on company datasource: {company_datasource.company_datasource_id}")
    progress_info = calculate_company_datasource_progress_details(
        etl_request=etl_request,
        logger=logger,
        session=session
    )

    # Publish to pubsub
    payload = {
        'event': 'progress',
        'company_datasource_id': company_datasource.company_datasource_id,
        'progress': progress_info.get('total'),
        'details': progress_info.get('details'),
        'data_type': data_type
    }

    payload = json.dumps(payload)
    payload = payload.encode('utf-8')
    publisher = pubsub_v1.PublisherClient()
    project_id = config.GCP_PROJECT
    topic_id = config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_PROGRESS
    topic_path = publisher.topic_path(project_id, topic_id)

    logger.info(f"Publishing a message to {topic_id}: {payload}")
    future = publisher.publish(topic_path, payload)
    logger.info(
        f"Published message to {topic_id}, result: {future.result()}")


@auto_session
@auto_logger
def handle_after_task(received_message, session=None, logger=None):
    payload = json.loads(received_message.message.data)
    assert payload is not None, "Cannot process None payload"

    data_transform_types = ['crawl', 'preprocess', 'translate', 'load']

    if payload.get('type') in data_transform_types:
        return handle_after_task_transform(received_message, session=session, logger=logger)
    else:
        raise Exception(f"Not supported payload type={payload.get('type')}")


@auto_session
@auto_logger
def handle_after_task_transform(received_message, session=None, logger=None):
    payload = json.loads(received_message.message.data)
    publish_time = received_message.message.publish_time
    publish_time = publish_time.astimezone()

    step_type = payload.get('type')
    event = payload.get('event')
    batch_id = payload.get('batch').get('batch_id')
    step_id = payload.get('step').get('step_id')
    step_detail_id = payload.get('step_detail').get('step_detail_id')
    item_count = payload.get('item_count')
    data_type = payload.get("batch").get("meta_data").get("data_type")
    error = payload.get('error', "")
    error = error[:255]  # Truncate long error message

    if not event or not step_type or not batch_id or not step_id or not step_detail_id:
        logger.info(f"Invalid payload")
        return

    if event not in ['finish', 'fail']:
        logger.info(f"Unsupported Pub/Sub event: {event}")
        return

    # 1. Update current task status
    logger.info(
        f"Updating status: {step_detail_id} with new status {EVENT_TO_STATUS.get(event)}")

    step_detail = None

    try:
        step_detail = session.query(ETLStepDetail).filter(
            ETLStepDetail.step_detail_id == step_detail_id).with_for_update().one()
    except Exception as e:
        logger.info(f"Unable select with_update step detail {step_detail_id}.")
        logger.error(f"Error: {e}")
        session.rollback()

    if not step_detail:
        logger.info(f"Unable to find step detail {step_detail_id}")
        return

    updated_time = step_detail.updated_at.astimezone()
    if updated_time >= publish_time:
        logger.info(
            f"Step detail {step_detail_id} newest than current progress. {updated_time.isoformat()} >= {publish_time.isoformat()}")
        session.commit()
        return

    this_batch: ETLCompanyBatch = session.query(ETLCompanyBatch).get(batch_id)
    this_request: ETLRequest = session.query(ETLRequest).get(this_batch.request_id)
    this_step: ETLStep = session.query(ETLStep).get(step_id)
    
    if item_count is not None:
        step_detail.item_count = item_count

    step_detail.error_message = error
    step_detail.status = EVENT_TO_STATUS.get(event) or step_detail.status
    step_detail.updated_at = publish_time
    if event == 'finish':
        logger.info(
            f"Step Detail {step_detail.step_detail_id} is finished. Total progress: {step_detail.progress_total}")
        step_detail.progress_current = step_detail.progress_total

    elif event == 'fail':
        # ETL ERROR FLOW
        publish_error_details(
            batch=this_batch,
            step=this_step,
            step_detail=step_detail,
            error_time=int(dt.datetime.now().timestamp()),
            error_message=error,
            additional_info=None,
            session=session,
            logger=logger
        )

        # Notify error message to case study error topic
        etl_company_datasource = (
            session
            .query(ETLCompanyDatasourceCommon)
            .filter(
                and_(
                    ETLCompanyDatasourceCommon.request_id == this_request.request_id,
                    ETLCompanyDatasourceCommon.data_type == this_request.data_type
                )
            )
            .first()
        )
        company_datasource_id = None if not etl_company_datasource else etl_company_datasource.company_datasource_id
        payload = {
            'event': 'error',
            'company_datasource_id': company_datasource_id,
            'error': error,
            'error_code': 'IN_PROGRESS',
            'data_type' : data_type
        }
        payload = json.dumps(payload)
        payload = payload.encode('utf-8')
        publisher = pubsub_v1.PublisherClient()
        topic_id = config.GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR
        topic_path = publisher.topic_path(config.GCP_PROJECT, topic_id)
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(f"Published message to {topic_id}, result: {future.result()}")

    session.commit()

    # 2. Handle next step
    next_step_type = NEXT_STEP_TYPES.get(step_type)
    if not next_step_type:
        logger.info(f"No next step for step_type: {step_type}")
        return

    next_step = session.query(ETLStep).filter(ETLStep.batch_id == batch_id).filter(
        ETLStep.step_type == next_step_type).first()

    next_step_name = step_detail.step_detail_name.replace(f"{step_type}-", f"{next_step_type}-")
    next_step_detail = session.query(ETLStepDetail).filter(ETLStepDetail.step_id == next_step.step_id).filter(
        ETLStepDetail.step_detail_name == next_step_name).first()

    if not next_step_detail:
        logger.info(
            f"Creating next step with type {next_step_type}...")

        meta_data = {**(step_detail.meta_data or {})}
        meta_data['source_name'] = step_detail.step_detail_name

        next_step_detail = ETLStepDetail(
            step_detail_name=step_detail.step_detail_name.replace(
                f"{step_type}-", f"{next_step_type}-"),
            paging=step_detail.paging,
            request_id=step_detail.request_id,
            step_id=next_step.step_id,
            batch_id=batch_id,
            status='waiting',
            file_id=step_detail.file_id,
            item_count=step_detail.item_count,
            progress_total=step_detail.item_count,
            progress_current=0,
            created_at=publish_time,
            updated_at=publish_time,
            lang=step_detail.lang,
            meta_data=meta_data
        )
        session.add(next_step_detail)
        session.commit()
    else:
        logger.info(f"Found existing next step {next_step_detail.step_id} : {next_step_detail.step_detail_name}")

    if event == "fail":
        logger.info(f"Current step detail {step_detail.step_detail_id} is failed. Marked all related to {event}.")
        next_step_detail.status = 'completed with error'
        session.commit()

        # Publish to self for fail propagate
        payload = {
            'type': next_step_type,
            'event': event,
            'request': this_request.to_json(),
            'batch': this_batch.to_json(),
            'step': next_step.to_json(),
            'step_detail': next_step_detail.to_json()
        }

        payload = json.dumps(payload)
        payload = payload.encode('utf-8')
        publisher = pubsub_v1.PublisherClient()
        topic_id = config.GCP_PUBSUB_TOPIC_AFTER_TASK
        topic_path = publisher.topic_path(config.GCP_PROJECT, topic_id)

        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(
            f"Published message to {topic_id}, result: {future.result()}")

    elif event == 'finish' and next_step_detail.status != 'finished':
        # Only next step detail is not finished 
        next_step_detail.status = 'waiting'
        session.commit()

        # Publish current message to next step pubsub topic
        payload = {
            'type': next_step_type,
            'event': next_step_type,
            'request': this_request.to_json(),
            'batch': this_batch.to_json(),
            'step': next_step.to_json(),
            'step_detail': next_step_detail.to_json(),
            'payload': this_request.payload,
        }
        payload = json.dumps(payload)
        payload = payload.encode('utf-8')
        publisher = pubsub_v1.PublisherClient()
        topic_id = STEP_TYPE_TO_TOPICS.get(next_step_type)
        topic_path = publisher.topic_path(config.GCP_PROJECT, topic_id)

        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(
            f"Published message to {topic_id}, result: {future.result()}")


def publish_error_details(
    batch: ETLCompanyBatch,
    step: ETLStep,
    step_detail: ETLStepDetail,
    error_time,
    error_message,
    additional_info=None,
    session=None,
    logger=None
):
    """
    Construct a valid payload and publish to error topic
    """
    # get necessary info
    data_type = batch.meta_data["data_type"]
    data_version = batch.data_version
    info_dict = additional_info if additional_info else {}
    total_step_details_count = (
        session
        .query(ETLStepDetail)
        .filter(ETLStepDetail.step_id == step.step_id)
        .count()
    )
    
    # determine error
    if step.step_type.lower() == "crawl":
        severity = "MAJOR"
    else:
        severity = "MINOR"
    
    publish_error_message(
        project_id=config.GCP_PROJECT,
        topic_id=config.GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR,
        company_datasource_id=batch.company_datasource_id,
        batch_id=batch.batch_id,
        step_detail_id=step_detail.step_detail_id,
        company_id=batch.company_id,
        company_name=batch.company_name,
        source_id=batch.source_id,
        source_name=batch.source_name,
        data_type=data_type,
        error_source=step.step_type,
        error_code="IN_PROGRESS",
        severity=severity,
        exception=error_message,
        total_step_details=total_step_details_count,
        error_time=error_time,
        data_version=data_version,
        additional_info=info_dict,
        logger=logger
    )
