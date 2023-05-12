from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestCrawlUploadedCSV(unittest.TestCase):
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
            batch_name="beanstalk-g2-20210203",
            status="running",
            company_name="Beanstalk",
            source_name="G2",
            source_code="G2",
            source_type="csv",
            url="gs://etl-datasource/upload/cs_000/csv_upload_sample_400k.csv",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"beanstalk-g2-20210203",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_detail_1 = ETLStepDetail(
            step_detail_name="crawl-beanstalk-g2-20201201-f001",
            status="running",
            paging="1:1",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_detail_1)
        self.session.commit()

        etl_detail_2 = ETLStepDetail(
            step_detail_name="crawl-beanstalk-g2-20201201-f002",
            status="running",
            paging="2:2",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=2,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_detail_2)
        self.session.commit()

        self.step = etl_step
        self.batch = batch
        self.ids = [etl_detail_1.step_detail_id, etl_detail_2.step_detail_id]

    def test_helper_crawl(self):
        from crawlers import uploaded_csv

        # Run the function
        task = {
            "url": "gs://etl-datasource/upload/cs_000/csv_upload_sample_400k.csv",
            "batch_id": self.batch.batch_id,
            "strategy": "csv",
            "ids": self.ids,
        }
        uploaded_csv.execute_task(task)

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 2, "Must have tasks in task dir"
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
