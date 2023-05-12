"""init_pubsub_topic

Revision ID: c76821838ac4
Revises: 8f3b4898d4b8
Create Date: 2021-01-13 11:35:27.264608

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import pubsub_v1

import config


# revision identifiers, used by Alembic.
revision = 'c76821838ac4'
down_revision = '8f3b4898d4b8'
branch_labels = None
depends_on = None


TOPICS = [
    'requests',
    'responses',
    'after_task',
    'progress',
    'cs_progress',
    'dispatch',
    'crawl',
    'preprocess',
    'translate',
    'nlp',
    'load',
    'export',
    'leo_backend_work_task',
]

SUBSCRITIONS = {
    'requests': [
        'requests_webplatform',
    ],
    'responses': [
        'responses_exporters',
        'responses_webplatform'
    ],
    'after_task': [
        'after_task_masters'
    ],
    'progress': [
        'progress_masters'
    ],
    'cs_progress': [
        'cs_progress_webplatform'
    ],
    'dispatch': [
        'dispatch_dispatchers'
    ],
    'crawl': [
        'crawl_crawlers'
    ],
    'preprocess': [
        'preprocess_preprocessors'
    ],
    'translate': [
        'translate_translators'
    ],
    'nlp': [
        'nlp_nlpers'
    ],
    'load': [
        'load_loaders'
    ],
    'export': [
        'export_exporters'
    ],
    'leo_backend_work_task': [
        'leo_backend_work_task_sub'
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

    for name in TOPICS:
        topic_id = f"{config.PREFIX}_{name}"
        topic_path = publisher.topic_path(project_id, topic_id)
        publisher.delete_topic(request={"topic": topic_path})
        print(f"Deleted topic {topic_path}")
