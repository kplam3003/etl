import os
import socket
import json
import time
import logging
import config
import helpers
from pathlib import Path
from gcloud import storage
from database import Session, ETLStepDetail
import shutil
import utils


logger = utils.load_logger()

if __name__ == "__main__":
    while True:
        crawler_id = config.CRAWLER_ID
        crawler_file = f"{config.TASKS_DIR}/{crawler_id}"
        if os.path.isfile(crawler_file):
            # File exists, I have tasks, let do it
            logger.debug(f"Tasks found {crawler_file}")
            helpers.execute_tasks(crawler_file)
            shutil.move(crawler_file, f"{crawler_file}.crawled")
        else:
            logger.info(f"Waiting for tasks...")
        time.sleep(30)
