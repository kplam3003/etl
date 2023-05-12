"""create_pubsub_wordfrequency

Revision ID: 3c3f92ddaacd
Revises: 466a8b639dbc
Create Date: 2021-04-05 08:53:14.565799

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import pubsub_v1
import config

# revision identifiers, used by Alembic.
revision = '3c3f92ddaacd'
down_revision = '466a8b639dbc'
branch_labels = None
depends_on = None


TOPICS = [
    'keyword_extract',
    'word_frequency',
    
]

SUBSCRITIONS = {
    'keyword_extract': [
        'keyword_extract_worker'
    ],
    'word_frequency': [
        'word_frequency_worker'
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
    project_id = config.GCP_PROJECT_ID
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    for name in TOPICS:
        topic_id = f"{config.PREFIX}_{name}"
        topic_path = publisher.topic_path(project_id, topic_id)
        publisher.delete_topic(request={"topic": topic_path})
        print(f"Deleted topic {topic_path}")
        for sub in SUBSCRITIONS.get(name):
            subscription_id = f"{config.PREFIX}_{sub}"
            subscription_path = subscriber.subscription_path(project_id, subscription_id)
            subscriber.delete_subscription(request={"subscription": subscription_path})
            print(f"Deleted subscription: {subscription_path}.")
