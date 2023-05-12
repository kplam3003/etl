import os
import socket
import json
import time
import config
import helpers
import utils
from database import ETLCompanyBatch, auto_session

logger = utils.load_logger()


@auto_session
def execute(session=None):
    logger.info(f"Loading batch name: {config.BATCH_NAME}, id: {config.BATCH_ID}")
    batch = session.query(ETLCompanyBatch).get(config.BATCH_ID)
    logger.info(f"Found batch: {batch} id: {batch and batch.batch_id}")

    while True:
        # Check if already has running step, then watch for step detail finished
        if helpers.has_running_crawl_step(batch, session=session):
            logger.info(f"Watching for finished task")
            helpers.check_and_update_step_status(batch, session=session)
        else:
            logger.info(f"No step there, generating tasks")
            helpers.generate_steps(batch, session=session)

        time.sleep(30)


if __name__ == "__main__":
    try:
        execute()
    except Exception as e:
        logger.exception(f"[FATAL] Dispatcher fails: {e}")
        raise e
