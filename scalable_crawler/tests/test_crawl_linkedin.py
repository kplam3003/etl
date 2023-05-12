from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import (
    Session,
    ETLCompanyBatch,
    ETLRequest,
    ETLStep,
    ETLStepDetail,
    ETLDatasource,
)
import helpers
import os
import shutil


class TestCrawlLinkedIn(unittest.TestCase):
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
            batch_name="manulife-linkedin-20210325",
            status="running",
            company_name="Manulife",
            source_name="LinkedIn",
            source_code="LinkedIn",
            url="https://www.linkedin.com/jobs/manulife-jobs-worldwide?f_C=3830899%2C1159822%2C887350%2C1620532%2C2690%2C4994980%2C2691&trk=nav_type_jobs&position=1&pageNum=0",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_data_source = ETLDatasource(
            batch_id=batch.batch_id,
            provider="luminati",
            provider_job_id="j_kno3c7721bgxfuowdi",
        )
        self.session.add(etl_data_source)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"crawl-manulife-linkedin",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_step_detail_job = ETLStepDetail(
            step_detail_name="job-crawl-manulife-linkedin-f001",
            status="running",
            paging="1:34",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            lang="en",
            meta_data={
                "version": "1.0",
                "data_type": "job",
                "url": "https://www.linkedin.com/jobs/td-ameritrade-jobs-worldwide?f_C=9385%2C2617029%2C12648%2C3388&trk=top-card_top-card-primary-button-top-card-primary-cta&position=1&pageNum=0",
            },
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_job)
        self.session.commit()

        etl_step_detail_overview = ETLStepDetail(
            step_detail_name="job-crawl-manulife-linkedin-f001",
            status="running",
            paging="1:1",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            lang="en",
            meta_data={
                "version": "1.0",
                "data_type": "overview",
                "url": "https://ca.linkedin.com/company/manulife-financial",
            },
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_overview)
        self.session.commit()

        self.request = request
        self.batch = batch
        self.step = etl_step
        self.etl_step_detail_job = etl_step_detail_job
        self.etl_step_detail_overview = etl_step_detail_overview

    def test_crawl_jobs(self):
        from crawlers import crawl_linkedin_task

        url = self.etl_step_detail_job.meta_data.get("url")

        task = {
            "url": url,
            "batch_id": self.batch.batch_id,
            "ids": [self.etl_step_detail_job.step_detail_id],
            "strategy": "linkedin",
        }

        # Run the function
        crawl_linkedin_task(task, session=self.session)

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 1, "Must have tasks in task dir"
        )

    def test_crawl_overview(self):
        from crawlers import crawl_linkedin_task

        url = self.etl_step_detail_job.meta_data.get("url")

        task = {
            "url": url,
            "batch_id": self.batch.batch_id,
            "ids": [self.etl_step_detail_overview.step_detail_id],
            "strategy": "luminati",
        }

        # Run the function
        crawl_linkedin_task(task, session=self.session)

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
