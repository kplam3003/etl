from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateGartnerSteps(unittest.TestCase):
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
            batch_name="messenger-gartner-20201229",
            status="running",
            company_name="Box",
            source_name="Gartner",
            source_code="Gartner",
            url="https://www.gartner.com/reviews/market/conversational-platforms/vendor/facebook/product/messenger-platform",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "review"},
        )
        self.session.add(batch)
        self.session.commit()

        self.request = request
        self.batch = batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = batch.batch_name

    def test_helper_genereate_review_steps(self):
        # Run the function
        helpers.generate_steps(self.batch)
        print("pass")

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
