"""Add nlp index pubsub

Revision ID: 5f70f57c4a70
Revises: 2858c236e4fa
Create Date: 2021-06-07 12:12:00.760423

"""
from alembic import op
import sqlalchemy as sa
import config
from google.cloud import pubsub_v1


# revision identifiers, used by Alembic.
revision = '5f70f57c4a70'
down_revision = '2858c236e4fa'
branch_labels = None
depends_on = None


TOPICS = [
    'nlp_index'
]

SUBSCRITIONS = {
    'nlp_index': [
        'nlp_indexer'
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
