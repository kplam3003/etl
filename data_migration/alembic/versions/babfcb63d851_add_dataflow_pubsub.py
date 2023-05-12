"""add dataflow pubsub

Revision ID: babfcb63d851
Revises: 3d905343055f
Create Date: 2021-04-06 15:12:16.860089

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import pubsub_v1

import config


# revision identifiers, used by Alembic.
revision = 'babfcb63d851'
down_revision = '3d905343055f'
branch_labels = None
depends_on = None


TOPICS = [
    'data_flow'
]

SUBSCRITIONS = {
    'data_flow': [
        'data_flow_masters'
    ]
}


def upgrade():
    from google.api_core.exceptions import AlreadyExists
    
    project_id = config.GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    for name in TOPICS:
        try:
            topic_id = f"{config.PREFIX}_{name}"
            topic_path = publisher.topic_path(project_id, topic_id)
            topic = publisher.create_topic(request={"name": topic_path})
            print(f"Created topic {topic_path}")
        except AlreadyExists as error:
            print(f"Topic {topic_path} already exits!")

        for sub in SUBSCRITIONS.get(name):
            try:
                subscription_id = f"{config.PREFIX}_{sub}"
                subscription_path = subscriber.subscription_path(project_id, subscription_id)
                subscription = subscriber.create_subscription(
                        request={"name": subscription_path, "topic": topic_path}
                    )
                print(f"Created subscription {subscription_path}")
            except AlreadyExists as error:
                print(f"Topic {topic_path} already exits!")


def downgrade():
    pass
