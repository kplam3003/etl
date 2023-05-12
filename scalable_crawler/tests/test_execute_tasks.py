from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil
import json
from google.cloud import storage


class TestExecuteTasks(unittest.TestCase):
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
            batch_name="paypal-capterra-20201201",
            status="running",
            company_name="Paypal",
            source_name="Capterra",
            url="https://www.capterra.com/p/207944/PayPal3/",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"crawl-paypal-capterra",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_step_detail_1 = ETLStepDetail(
            step_detail_name="crawl-paypal-capterra-f001",
            status="running",
            paging="1:2",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_1)
        self.session.commit()

        etl_step_detail_2 = ETLStepDetail(
            step_detail_name="crawl-paypal-capterra-f002",
            status="running",
            paging="3:4",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=2,
            created_at=dt.datetime.now(),
        )

        self.session.add(etl_step_detail_2)
        self.session.commit()
        self.batch = batch
        self.step_detail_1 = etl_step_detail_1
        self.step_detail_2 = etl_step_detail_2

        # Generate task file to be able to test
        with open(f"{config.TASKS_DIR}/{config.CRAWLER_ID}", "w+") as f:
            f.writelines(
                json.dumps(
                    {
                        "url": batch.url,
                        "step_detail_id": etl_step_detail_1.step_detail_id,
                    }
                )
                + "\n"
            )
            f.writelines(
                json.dumps(
                    {
                        "url": batch.url,
                        "step_detail_id": etl_step_detail_2.step_detail_id,
                    }
                )
                + "\n"
            )

    def test_execute_tasks(self):
        # Run the function
        helpers.execute_tasks(f"{config.TASKS_DIR}/{config.CRAWLER_ID}")

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 2, "Must have output in out dir"
        )

    def tearDown(self):
        # Clean output dir
        for f in os.listdir(config.OUTPUT_DIR):
            os.remove(f"{config.OUTPUT_DIR}/{f}")

        # Clean out task dir
        for f in os.listdir(config.TASKS_DIR):
            os.remove(f"{config.TASKS_DIR}/{f}")

        # Clean cloud storage file
        storage_client = storage.Client()
        bucket = storage_client.bucket(config.GCP_STORAGE_BUCKET)
        for step_detail in [self.step_detail_1, self.step_detail_2]:
            dst_file = f"crawl/{self.batch.batch_name.strip()}/{step_detail.step_detail_name.strip()}.csv"
            blob = bucket.blob(dst_file)
            blob.delete()

        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()
