from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateGooglePlaySteps(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()

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
            batch_name="asphalt-xtreme-googleplay-20201204",
            status="running",
            company_name="asphalt-xtreme",
            source_name="GooglePlay",
            source_code="GooglePlay",
            url="https://play.google.com/store/apps/details?id=com.gameloft.android.ANMP.GloftMOHM",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        self.request = request
        self.batch = batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = batch.batch_name

    def test_helper_genereate_steps(self):
        # Run the function
        helpers.generate_googleplay_steps(self.batch)

    def test_generate_step_with_preinit_step(self):
        batch = self.batch

        etl_step = ETLStep(
            step_name=f"crawl-{batch.company_name.lower()}-{batch.source_name.lower()}",
            status="waiting",
            created_at=dt.datetime.today(),
            request_id=batch.request_id,
            batch_id=batch.batch_id,
            step_type="crawl",
        )
        self.session.add(etl_step)
        self.session.commit()

        # Run the function
        helpers.generate_steps(self.batch)

        # Must have two task file in the TASK_DIR
        self.assertEquals(
            len(os.listdir(config.TASKS_DIR)),
            len(config.CRAWLER_IDS),
            "Must have tasks in task dir",
        )

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
