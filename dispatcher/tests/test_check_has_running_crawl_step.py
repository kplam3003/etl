from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestCheckForRunningCrawlStep(unittest.TestCase):
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
            batch_name="paypal-googleplay-20201201",
            status="running",
            company_name="Paypal",
            source_name="GooglePlay",
            url="https://play.google.com/store/apps/details?id=com.paypal.android.p2pmobile",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        self.batch = batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = batch.batch_name

    def test_check_has_no_running_crawl_step(self):
        # Insert step that is not crawl
        step = ETLStep(
            step_name=f"translate-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}",
            batch_id=self.batch.batch_id,
            created_at=dt.datetime.now(),
            request_id=self.batch.request_id,
            status="running",
        )
        self.session.add(step)
        self.session.commit()

        # Run the function
        has_crawl = helpers.has_running_crawl_step(self.batch)

        self.assertEquals(has_crawl, False)

    def test_check_has_running_crawl_step(self):
        # Insert step that is not crawl
        step = ETLStep(
            step_name=f"crawl-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}",
            batch_id=self.batch.batch_id,
            created_at=dt.datetime.now(),
            request_id=self.batch.request_id,
            status="running",
        )
        self.session.add(step)
        self.session.commit()

        step_detail = ETLStepDetail(
            request_id=self.batch.request_id,
            batch_id=self.batch.batch_id,
            step_detail_name=f"{step.step_name}-20201204-f001",
            status="running",
            step_id=step.step_id,
            file_id=1,
            paging="1:10",
            created_at=dt.datetime.today(),
            updated_at=dt.datetime.today(),
        )
        self.session.add(step_detail)
        self.session.commit()

        # Run the function
        has_crawl = helpers.has_running_crawl_step(self.batch)

        self.assertEquals(has_crawl, True)

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
