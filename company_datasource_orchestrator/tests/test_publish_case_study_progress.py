import sys
sys.path.append('../')

import unittest
import datetime as dt
from data import dump_data
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers

step_types = ['crawl', 'preprocess', 'translate', 'nlp', 'load']

class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.session = Session()
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()

        def now(): return dt.datetime.utcnow()

        dump_data(session=self.session, model=ETLRequest, data=[
            ['request_id', 'case_study_id', 'case_study_name', 'status', 'created_at', 'updated_at'],
            [1, 1, 'Case Name 1', 'finished', now(), now()],
            [2, 2, 'Case Name 2', 'waiting', now(), now()]
        ])

        dump_data(session=self.session, model=ETLCompanyBatch, data=[
            ['batch_id', 'request_id', 'batch_name', 'status', 'company_id', 'company_name', 'source_id', 'source_name',
             'source_code', 'url', 'nlp_pack', 'nlp_type', 'created_at', 'updated_at'],
            [11, 1, '101-2-20210104', 'finished', 101, 'Paypal Test', 2, 'Capterra',
             'Capterra', 'https://www.capterra.com/p/207944/PayPal3/', 'VoC Banking', 'VoC', now(), now()],
            [12, 1, '102-2-20210104', 'running', 102, 'Payoneer Test', 2, 'Capterra',
             'Capterra', 'https://www.capterra.com/p/187086/Payoneer/', 'VoC Banking', 'VoC', now(), now()],
            [21, 2, '101-2-20210104', 'waiting', 101, 'Paypal Exist Test', 2, 'Capterra',
             'Capterra', 'https://www.capterra.com/p/207944/PayPal3/', 'VoC Banking', 'VoC', now(), now()],
        ])

        # Generate etl steps
        step_data = [
            *[[1, 11, int(f"11{ix + 1}"), f'{st}-101-2-20210104', 'finished', st, now(), now()] for ix, st in
              enumerate(step_types)],
            *[[1, 12, int(f"12{ix + 1}"), f'{st}-102-2-20210104', 'running', st, now(), now()] for ix, st in
              enumerate(step_types)]
        ]
        dump_data(session=self.session, model=ETLStep, data=[
            ['request_id', 'batch_id', 'step_id', 'step_name', 'status', 'step_type', 'created_at', 'updated_at'],
            *step_data
        ])

        dump_data(session=self.session, model=ETLStepDetail, data=[
            ['request_id', 'batch_id', 'step_id', 'step_detail_id', 'step_detail_name', 'status', 'paging', 'file_id',
             'created_at', 'updated_at'],
            *[[req_id, batch_id, step_id, int(f'{step_id}{ix + jx}'), f'{step_name}-f{ix + jx:03d}', status, '1:2', ix,
               now(), now()] for jx in range(3)
              for ix, (req_id, batch_id, step_id, step_name, status, step_type, *_) in enumerate(step_data)]
        ])

    def test_publish_cs_progress(self):
        helpers.publish_case_study_progress(request_id=1, session=self.session)

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()


if __name__ == '__main__':
    unittest.main()
