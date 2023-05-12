import sys

sys.path.append("../")
from dotenv import load_dotenv

load_dotenv()
import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestCrawReddit(unittest.TestCase):
    def setUp(self):
        self.session = Session()

        request = ETLRequest(
            case_study_id=1,
            case_study_name="South Retail",
            status="running",
            created_at=dt.datetime.now(),
        )
        self.session.add(request)
        # self.session.commit()

        batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="revolut-reddit-20206171",
            status="running",
            company_name="Revolut",
            source_name="Reddit",
            source_code="Reddit",
            url="hhttps://www.reddit.com/r/Revolut/",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"revolut-reddit-20206171",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_step_detail = ETLStepDetail(
            step_detail_name="crawl-revolut-reddit-20206171-f001",
            status="running",
            paging="1:2",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail)
        self.session.commit()

        self.request = request
        self.batch = batch
        self.step = etl_step
        self.step_detail = etl_step_detail

    def test_helper_crawl(self):
        from crawlers import reddit

        # Run the function
        reddit.execute_task(
            {
                "url": self.batch.url,
                "ids": [self.step_detail.step_detail_id],
                "batch_id": self.batch.batch_id,
            }
        )

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 1, "Must have tasks in task dir"
        )

    def tearDown(self):
        # Clean output dir
        for f in os.listdir(config.OUTPUT_DIR):
            os.remove(f"{config.OUTPUT_DIR}/{f}")

        # Clean out task dir
        for f in os.listdir(config.TASKS_DIR):
            os.remove(f"{config.TASKS_DIR}/{f}")

        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
