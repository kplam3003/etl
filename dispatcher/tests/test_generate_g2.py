from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateG2Steps(unittest.TestCase):
    def setUp(self):
        self.session = Session()

        request = ETLRequest(
            case_study_id=1,
            case_study_name="South Retail",
            status="running",
            created_at=dt.datetime.now(),
        )
        self.session.add(request)
        self.session.commit()

        batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="beanstalk-g2-20201229",
            status="running",
            company_name="Beanstalk",
            source_name="G2",
            source_code="G2",
            url="https://www.g2.com/products/aws-elastic-beanstalk/reviews",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        self.request = request
        self.batch = batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = batch.batch_name

    def test_helper_genereate_g2_steps(self):
        # Run the function
        helpers.generate_steps(self.batch)

        # Must have two task file in the TASK_DIR
        self.assertEquals(
            len(os.listdir(config.TASKS_DIR)),
            len(config.CRAWLER_IDS),
            "Must have tasks in task dir",
        )

    def tearDown(self):
        # Clean task directory
        for crawler_id in config.CRAWLER_IDS:
            try:
                os.remove(f"{config.TASKS_DIR}/{crawler_id}")
            except:
                pass

        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
