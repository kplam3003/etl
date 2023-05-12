import sys

sys.path.append("../")

from google.cloud import pubsub_v1
import datetime as dt
import time
import config
import json
import helpers
from database import (
    ETLCompanyBatch,
    ETLStep,
    auto_session,
)
from core.logger import auto_logger


@auto_session
@auto_logger
def listen_new_batches(session=None, logger=None):
    project_id = config.GCP_PROJECT
    subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_DISPATCHER
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

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
            batch_id = data.get("batch_id")

            logger.info(f"Generating step details for batch: {batch_id}")
            etl_batch = session.query(ETLCompanyBatch).get(batch_id)
            batch_metadata = etl_batch.meta_data

            if not etl_batch:
                logger.info(f"Unable to find batch {batch_id}")
                continue

            exception = None
            for sleep in [1.0, 2.0]:
                try:
                    helpers.generate_steps(etl_batch, session=session)
                    exception = None
                    break
                except Exception as e:
                    msg = (
                        f"Error while generating steps !!, retrying... in {sleep} secs"
                    )
                    logger.info(f"[ERROR] {msg}", exc_info=True)
                    logger.exception(msg)
                    exception = e
                    error_time = int(dt.datetime.now().timestamp())
                    time.sleep(sleep)

            if exception:
                logger.error(
                    f"[ERROR] unable to crawl batch_id: {batch_id}, exception: {exception}"
                )
                etl_batch.status = "completed with error"
                etl_steps = (
                    session.query(ETLStep).filter(ETLStep.batch_id == batch_id).all()
                )
                for etl_step in etl_steps:
                    etl_step.status = "completed with error"

                session.commit()

                # publish ETL Internal Error message
                helpers.publish_dispatcher_error_details(
                    batch=etl_batch,
                    error_time=error_time,
                    exception=exception,
                    additional_info={"url": etl_batch.url},
                    logger=logger,
                )

                raise exception


if __name__ == "__main__":
    listeners = {
        "listen_new_batches": listen_new_batches,
    }

    import argparse

    parser = argparse.ArgumentParser(description="Listen to a PubSub subscription.")
    parser.add_argument("--listener", help="Name of the listener")
    args = parser.parse_args()
    runner = listeners.get(args.listener)
    if not runner:
        print(f"No listener found with name: {args.listener}")
    else:
        runner()
