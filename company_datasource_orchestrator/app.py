import sys

import postgres_helpers

sys.path.append('../')

import os
from celery import Celery
from google.cloud import pubsub_v1
import config
import helpers
import json
import time
import requests
import datetime as dt
from database import auto_session, ETLStep, ETLCompanyBatch, ETLStepDetail, ETLRequest
from sqlalchemy import or_
from core.logger import auto_logger

MQ_HOST = os.environ.get('MQ_HOST', 'localhost')

app = Celery('app', broker=f'amqp://{MQ_HOST}')


@auto_session
@auto_logger
def listen_waiting_company_batches(session=None, logger=None):
    logger.info(f"Finding waiting batches to run...")

    # Check whether have waiting batches to run
    waiting_batches = session.query(ETLCompanyBatch).filter(ETLCompanyBatch.status == "waiting").all() or []
    if len(waiting_batches) == 0:
        logger.info(f"No waiting batch.")
        return "No waiting batch to run"

    logger.info(f"Total waiting batches: {len(waiting_batches)}")
    publisher = pubsub_v1.PublisherClient()
    topic_id = config.GCP_PUBSUB_TOPIC_DISPATCH
    topic_path = publisher.topic_path(config.GCP_PROJECT, topic_id)

    for waiting_batch in waiting_batches:
        logger.info(f"Set batch {waiting_batch.batch_name} as running")
        helpers.update_batch(batch_id=waiting_batch.batch_id, status='running', session=session)

        # Generate a crawling step and a vm for it
        logger.info(f"Creating new crawl step to database")
        helpers.create_step(waiting_batch, step_type='crawl', session=session)

        logger.info(f"Creating new preprocess step to database")
        helpers.create_step(waiting_batch, step_type='preprocess', session=session)

        logger.info(f"Creating new translate step to database")
        helpers.create_step(waiting_batch, step_type='translate', session=session)

        logger.info(f"Creating new loader step to database")
        helpers.create_step(waiting_batch, step_type='load', session=session)

        # Publish dispatch message
        payload = json.dumps({"batch_id": waiting_batch.batch_id})
        payload = payload.encode('utf-8')
        logger.info(f"Publishing a message to {topic_id}: {payload}")
        future = publisher.publish(topic_path, payload)
        logger.info(f"Published message to {topic_id}, result: {future.result()}")

    return "Completed"


@auto_session
@auto_logger
def scale_vms(session=None, logger=None):
    # Check whether have running batches to scale vms
    running_batches = (
        session
        .query(ETLCompanyBatch)
        .filter(ETLCompanyBatch.status == "running")
        .all()
    )
    num_running_batches = len(running_batches)
    if num_running_batches == 0:
        logger.info(f"No running batch.")
        return

    # Get running crawl VMs
    response = helpers.list_vm()
    running_vms = response.get('items', [])
    num_running_vms = len(running_vms)
    logger.info(f"Total running crawler VMs: {num_running_vms}")
    
    # Get running crawl steps
    running_crawl_steps = (
        session
        .query(ETLStep)
        .filter(ETLStep.step_type == 'crawl')
        .filter(or_(ETLStep.status == 'waiting', ETLStep.status == 'running'))
        .all()
    )
    num_running_crawl_steps = len(running_crawl_steps)
    logger.info(f"Total running crawl jobs: {num_running_crawl_steps}")
    
    # NOT scale logics
    if not running_crawl_steps or num_running_crawl_steps <= 0:
        logger.info(f"There are no running crawling task")
        return

    if len(running_vms) >= num_running_crawl_steps:
        logger.info(f"There are enough running VMs for {num_running_crawl_steps} crawl jobs")
        return   

    if num_running_vms >= config.MAX_VM:
        logger.info(f"There are already {num_running_vms} running VMs out of max {config.MAX_VM}")
        return
    
    # scale logics
    num_vms_to_create = min(config.MAX_VM - num_running_vms, num_running_crawl_steps)
    logger.info(f"Creating {num_vms_to_create} VMs for {num_running_crawl_steps} crawl jobs...")
    for _ in range(num_vms_to_create):
        helpers.create_vm(step_type='crawl')
        logger.info(f"Created 1 VM")


@auto_session
@auto_logger
def clean_vms(session=None, logger=None):
    logger.info(f"Cleaning running vm")
    running_crawl_steps = (
        session
        .query(ETLStep)
        .filter(ETLStep.step_type == 'crawl')
        .filter(or_(ETLStep.status == 'waiting', ETLStep.status == 'running'))
        .all()
    )
    if len(running_crawl_steps) > 0:
        logger.info(f"There are still running crawling task.")
        return

    logger.info(f"No more crawl task running. Cleaning vm...")
    response = helpers.list_vm(step_type='crawl')
    crawl_vms = response.get('items', [])
    logger.info(f"Found {len(crawl_vms)} running VMs")
    for vm in crawl_vms:
        vm_name = vm.get('name')
        helpers.delete_vm(vm_name, step_type='crawl')
        logger.info(f"VM {vm_name} is deleted")


if __name__ == '__main__':
    apps = {
        'run_vm_task': listen_waiting_company_batches,
        'scale_vms': scale_vms,
        'clean_vm_task': clean_vms
    }

    import argparse

    parser = argparse.ArgumentParser(
        description='Listen to a PubSub subscription.')
    parser.add_argument('--app', help='Name of the app')
    args = parser.parse_args()
    runner = apps.get(args.app)
    if not runner:
        print(f"No app found with name: {args.app}")
    else:
        while True:
            runner()
            time.sleep(config.SLEEP)
