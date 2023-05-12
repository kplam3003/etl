"""add topic and subsc for internal ETL process error

Revision ID: c327919184b2
Revises: d0929a2b7554
Create Date: 2022-04-05 07:58:59.402129

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound
import config

# revision identifiers, used by Alembic.
revision = 'c327919184b2'
down_revision = 'd0929a2b7554'
branch_labels = None
depends_on = None

TOPIC = "internal_etl_error"

SUBSCRITION = "internal_etl_error_subscription"

def upgrade():
    project_id = config.GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    # create a topic
    try:
        topic_id = f"{config.PREFIX}_{TOPIC}"
        topic_path = publisher.topic_path(project_id, topic_id)
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Created topic {topic_path}")
    except AlreadyExists:
        print(f"Topic {topic_path} already exits!")
        
    # create a associated subscription
    subscriber = pubsub_v1.SubscriberClient()
    try:
        subscription_id = f"{config.PREFIX}_{SUBSCRITION}"
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        subscription = subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
        print(f"Created subscription {subscription_path}")
    except AlreadyExists as error:
        print(f"Subscription {topic_path} already exits!")

def downgrade():
    project_id = config.GCP_PROJECT_ID
    
    # delete subscription
    subscriber = pubsub_v1.SubscriberClient()
    try:
        subscription_id = f"{config.PREFIX}_{SUBSCRITION}"
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        subscriber.delete_subscription(request={"subscription": subscription_path})
        print(f"Deleted subscription: {subscription_path}.")
    except NotFound:
        print(f"Subscription {topic_path} not found!")
        
    # delete topic
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_id = f"{config.PREFIX}_{TOPIC}"
        topic_path = publisher.topic_path(project_id, topic_id)
        publisher.delete_topic(request={"topic": topic_path})
        print(f"Deleted topic {topic_path}")
    except NotFound:
        print(f"Topic {topic_path} not found!")
