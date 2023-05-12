"""Add new subscription for topic dev_responses

Revision ID: 4ee850398222
Revises: 13b8210289d3
Create Date: 2021-01-19 14:18:12.226552

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import pubsub_v1
import config


# revision identifiers, used by Alembic.
revision = '4ee850398222'
down_revision = 'c0b98cb515f6'
branch_labels = None
depends_on = None

TOPIC = 'responses'

SUBSCRITION = 'responses_operation'

def upgrade():
    project_id = config.GCP_PROJECT_ID
    subscriber = pubsub_v1.SubscriberClient()

    topic_path = subscriber.topic_path(project_id, f"{config.PREFIX}_{TOPIC}")
    subscription_path = subscriber.subscription_path(project_id, f"{config.PREFIX}_{SUBSCRITION}")
    try:
        subscription = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print("Subscription created: {}".format(subscription))
    except Exception as error:
        print(f"error: {error}") 
def downgrade():
    pass
