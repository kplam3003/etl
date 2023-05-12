"""add subscriber for company_datasource_error topic

Revision ID: 4218feaf8b9c
Revises: 7ef0bd01ee39
Create Date: 2021-08-27 11:58:24.485401

"""
from alembic import op
import sqlalchemy as sa

from google.api_core.exceptions import AlreadyExists

import config
from google.cloud import pubsub_v1


# revision identifiers, used by Alembic.
revision = '4218feaf8b9c'
down_revision = '7ef0bd01ee39'
branch_labels = None
depends_on = None


TOPIC_NAME = 'company_datasource_error'
SUBSCRIPTION_NAME = 'company_datasource_error_master'


def upgrade():
    project_id = config.GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_id = f"{config.PREFIX}_{TOPIC_NAME}"
    topic_path = publisher.topic_path(project_id, topic_id)
    
    try:
        # add environment prefix into subscription name for each environment
        subscription_id = f"{config.PREFIX}_{SUBSCRIPTION_NAME}"
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"Created subscription {subscription_path}")
    except AlreadyExists as error:
        print(f"Topic {topic_path} already exits!")


def downgrade():
    project_id = config.GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    subscription_id = f"{config.PREFIX}_{SUBSCRIPTION_NAME}"
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    subscriber.delete_subscription(request={"subscription": subscription_path})
    print(f"Deleted subscription: {subscription_path}.")

