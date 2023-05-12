from google.cloud import pubsub_v1
import json
import time

SLEEP_DEFAULT = 5


def publish(logger, gcp_product_id, topic, payload):
    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(gcp_product_id, topic)
    data = json.dumps(payload)

    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    result = future.result()
    logger.info(f"Published a message result={result}, payload={json.dumps(payload)}")


def subscribe(logger, gcp_product_id, gcp_pubsub_subscription, handle_task, max_messages=1):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(gcp_product_id, gcp_pubsub_subscription)
    with subscriber:
        while True:
            # The subscriber pulls a message.
            logger.info(f"Waiting for a message on {gcp_pubsub_subscription}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": max_messages}
            )
            
            if not response:
                time.sleep(SLEEP_DEFAULT)
                continue
            
            ack_ids = []
            messages = []
            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")
                ack_ids.append(received_message.ack_id)
                messages.append(received_message.message.data)
                
            # Acknowledges the received messages so they will not be sent again.
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

            for message in messages:
                data = json.loads(message, strict=False)
                handle_task(data)
    
