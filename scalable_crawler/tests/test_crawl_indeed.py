from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestCrawlIndeed(unittest.TestCase):
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
            batch_name="cepheid-indeed-20210325",
            status="running",
            company_name="Amazon",
            source_name="Indeed",
            source_code="Indeed",
            url="https://uk.indeed.com/cmp/Cepheid",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"crawl-cepheid-indeed",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_step_detail_review = ETLStepDetail(
            step_detail_name="review-crawl-cepheid-indeed-f001",
            status="running",
            paging="1:20",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            lang="eng",
            meta_data={
                "version": "1.0",
                "data_type": "review",
                "url": "https://uk.indeed.com/cmp/Cepheid/reviews",
            },
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_review)
        self.session.commit()

        etl_step_detail_job = ETLStepDetail(
            step_detail_name="job-crawl-cepheid-indeed-f002",
            status="running",
            paging="1:2",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            lang="eng",
            meta_data={
                "version": "1.0",
                "data_type": "job",
                "url": "https://uk.indeed.com/cmp/Cepheid/jobs",
            },
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_job)
        self.session.commit()

        etl_step_detail_overview = ETLStepDetail(
            step_detail_name="overview-crawl-cepheid-indeed-f001",
            status="running",
            paging="1:1",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            lang="eng",
            meta_data={
                "version": "1.0",
                "data_type": "overview",
                "url": "https://uk.indeed.com/cmp/Cepheid/reviews",
            },
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail_overview)
        self.session.commit()

        self.request = request
        self.batch = batch
        self.step = etl_step
        self.etl_step_detail_review = etl_step_detail_review
        self.etl_step_detail_job = etl_step_detail_job
        self.etl_step_detail_overview = etl_step_detail_overview

    def test_crawl_overview(self):
        from crawlers import crawl_indeed_task

        url = self.etl_step_detail_overview.meta_data.get("url")

        task = {
            "url": url,
            "batch_id": self.batch.batch_id,
            "ids": [self.etl_step_detail_overview.step_detail_id],
            "strategy": "indeed",
        }

        # Run the function
        crawl_indeed_task(task, session=self.session)

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 1, "Must have tasks in task dir"
        )

    def test_crawl_reviews(self):
        from crawlers import crawl_indeed_task

        url = self.etl_step_detail_review.meta_data.get("url")

        task = {
            "url": url,
            "batch_id": self.batch.batch_id,
            "ids": [self.etl_step_detail_review.step_detail_id],
            "strategy": "indeed",
        }

        # Run the function
        crawl_indeed_task(task, session=self.session)

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 1, "Must have tasks in task dir"
        )

    def test_crawl_jobs(self):
        from crawlers import crawl_indeed_task

        url = self.etl_step_detail_job.meta_data.get("url")

        task = {
            "url": url,
            "batch_id": self.batch.batch_id,
            "ids": [self.etl_step_detail_job.step_detail_id],
            "strategy": "indeed",
        }

        # Run the function
        crawl_indeed_task(task, session=self.session)

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
