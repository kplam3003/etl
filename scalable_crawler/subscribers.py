import sys

sys.path.append("../")

from google.cloud import pubsub_v1
import datetime as dt
import time
import config
import json
from crawlers import get_crawler
from database import (
    Session,
    ETLRequest,
    ETLCompanyBatch,
    ETLStep,
    ETLStepDetail,
    auto_session,
)
from core.logger import auto_logger


@auto_session
@auto_logger
def listen_crawl_task(session=None, logger=None):
    project_id = config.GCP_PROJECT_ID
    subscription_id = config.GCP_PUBSUB_SUBSCRIPTION_CRAWLERS
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

            logger.info(f"Crawling task: {data}")
            etl_batch = session.query(ETLCompanyBatch).get(batch_id)

            if not etl_batch:
                logger.error(f"Unable to find batch {batch_id}")
                continue

            code = (
                etl_batch.source_type
                if etl_batch.source_type == "csv"
                else etl_batch.source_code
            )
            execute_task = get_crawler(code)
            if not execute_task:
                logger.error(
                    f"Unable to find crawler for batch {batch_id} - source_code: {etl_batch.source_code} - source_type: {etl_batch.source_type}"
                )

            try:
                execute_task(data, session=session)
            except Exception as exc:
                logger.exception(
                    f"Unable to crawl data. Republish message to pub/sub after few seconds. Current data: {received_message.message.data}"
                )
                time.sleep(config.SLEEP)
                retry_total = data.get("retry_total", 0) + 1

                if retry_total > 2:
                    logger.warning(
                        f"Retried 2 times. Drop the message: {received_message.message.data}"
                    )
                    continue

                logger.warning(f"Current retrying times: {retry_total}, data: {data}")
                data["retry_total"] = retry_total
                data = json.dumps(data)
                data = data.encode("utf-8")

                publisher = pubsub_v1.PublisherClient()
                topic_path = publisher.topic_path(
                    config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_CRAWL
                )
                future = publisher.publish(topic_path, data)
                result = future.result()
                logger.info(f"Message published to Pub/Sub {result}")


if __name__ == "__main__":
    listeners = {
        "listen_crawl_task": listen_crawl_task,
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
