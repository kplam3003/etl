"""create_new_pubsub_for_company_datasource

Revision ID: 240a133a11b1
Revises: 58131feebbec
Create Date: 2021-08-04 14:55:08.584195

"""
from alembic import op
import sqlalchemy as sa
import config
from google.cloud import pubsub_v1

# revision identifiers, used by Alembic.
revision = '240a133a11b1'
down_revision = '58131feebbec'
branch_labels = None
depends_on = None


TOPICS = [
    'company_datasource_progress',
    'company_datasource_error'
]


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


def downgrade():
    pass
