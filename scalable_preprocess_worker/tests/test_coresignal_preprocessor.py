import sys

sys.path.append("../")
sys.path.append("../../")

import unittest

from coresignal_helpers import find_previous_next_company, check_starter_leaver


correct_output = [
    {
        "id": 2968352771,
        "company_id": 3057373,
        "company_name": "UAB Baltic aviation academy",
        "previous_coresignal_company_id": None,
        "next_coresignal_company_id": 3057373,
        "date_from": "2012-10-01",
        "date_to": "2013-02-01",
        "is_starter": True,
        "is_leaver": False,
    },
    {
        "id": 2968352770,
        "company_id": 3057373,
        "company_name": "UAB Baltic aviation academy",
        "previous_coresignal_company_id": 3057373,
        "next_coresignal_company_id": 3057373,
        "date_from": "2013-02-01",
        "date_to": "2013-10-01",
        "is_starter": False,
        "is_leaver": False,
    },
    {
        "id": 2968352769,
        "company_id": 3057373,
        "company_name": "UAB Baltic aviation academy",
        "previous_coresignal_company_id": 3057373,
        "next_coresignal_company_id": 3057373,
        "date_from": "2013-10-01",
        "date_to": "2016-04-01",
        "is_starter": False,
        "is_leaver": True,
    },
    {
        "id": 2968352768,
        "company_id": 3057373,
        "company_name": "BAA Training",
        "previous_coresignal_company_id": 3057373,
        "next_coresignal_company_id": 29103520,
        "date_from": "2017-11-01",
        "date_to": "2022-02-01",
        "is_starter": True,
        "is_leaver": True,
    },
    {
        "id": 2968352767,
        "company_id": 29103520,
        "company_name": "Coresignal",
        "previous_coresignal_company_id": 3057373,
        "next_coresignal_company_id": None,
        "leo_company_ids": [274],
        "date_from": "2022-02-01",
        "date_to": None,
        "is_starter": True,
        "is_leaver": False,
    },
]


class CapterraPreprocessorTest(unittest.TestCase):
    def setUp(self):
        self.ref_coresignal_company_ids = [29103520]
        self.leo_company_id = 274
        self.first_input = [
            {
                "id": 2968352771,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "date_from": "2012-10-01",
                "date_to": "2013-02-01",
            },
            {
                "id": 2968352770,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "date_from": "2013-02-01",
                "date_to": "2013-10-01",
            },
            {
                "id": 2968352769,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "date_from": "2013-10-01",
                "date_to": "2016-04-01",
            },
            {
                "id": 2968352768,
                "company_id": 3057373,
                "company_name": "BAA Training",
                "date_from": "2017-11-01",
                "date_to": "2022-02-01",
            },
            {
                "id": 2968352767,
                "company_id": 29103520,
                "company_name": "Coresignal",
                "date_from": "2022-02-01",
                "date_to": None,
            },
        ]

        self.second_input = [
            {
                "id": 2968352771,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "previous_coresignal_company_id": None,
                "next_coresignal_company_id": 3057373,
                "date_from": "2012-10-01",
                "date_to": "2013-02-01",
            },
            {
                "id": 2968352770,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "previous_coresignal_company_id": 3057373,
                "next_coresignal_company_id": 3057373,
                "date_from": "2013-02-01",
                "date_to": "2013-10-01",
            },
            {
                "id": 2968352769,
                "company_id": 3057373,
                "company_name": "UAB Baltic aviation academy",
                "previous_coresignal_company_id": 3057373,
                "next_coresignal_company_id": 3057373,
                "date_from": "2013-10-01",
                "date_to": "2016-04-01",
            },
            {
                "id": 2968352768,
                "company_id": 3057373,
                "company_name": "BAA Training",
                "previous_coresignal_company_id": 3057373,
                "next_coresignal_company_id": 29103520,
                "date_from": "2017-11-01",
                "date_to": "2022-02-01",
            },
            {
                "id": 2968352767,
                "company_id": 29103520,
                "company_name": "Coresignal",
                "previous_coresignal_company_id": 3057373,
                "next_coresignal_company_id": None,
                "leo_company_ids": [274],
                "date_from": "2022-02-01",
                "date_to": None,
            },
        ]

    def test_find_previous_next_company(self):
        first_output = find_previous_next_company(
            member_experiences=self.first_input,
            leo_company_id=self.leo_company_id,
            ref_coresignal_company_ids=self.ref_coresignal_company_ids,
        )
        for i, out in enumerate(first_output):
            experience_id = out["id"]
            inp_list = list(
                filter(lambda x: x["id"] == experience_id, self.first_input)
            )
            # sanity check
            assert (
                len(inp_list) == 1
            ), "output experience_id must match exactly one input experience entry"
            ref_out = correct_output[i]
            assert out["id"] == ref_out["id"], 'incorrect "id" field value'
            assert (
                out["previous_coresignal_company_id"]
                == ref_out["previous_coresignal_company_id"]
            ), 'incorrect "previous_coresignal_company_id" field value'
            assert (
                out["next_coresignal_company_id"]
                == ref_out["next_coresignal_company_id"]
            ), 'incorrect "next_coresignal_company_id" field value'
            assert out.get("leo_company_ids") == ref_out.get(
                "leo_company_ids"
            ), 'incorrect "leo_company_ids" field value'

    def test_check_starter_leaver(self):
        second_output = check_starter_leaver(
            member_experiences=self.first_input,
            leo_company_id=self.leo_company_id,
            ref_coresignal_company_ids=self.ref_coresignal_company_ids,
        )
        for i, out in enumerate(second_output):
            experience_id = out["id"]
            inp_list = list(
                filter(lambda x: x["id"] == experience_id, self.first_input)
            )
            # sanity check
            assert (
                len(inp_list) == 1
            ), "output experience_id must match exactly one input experience entry"
            ref_out = correct_output[i]
            assert out["id"] == ref_out["id"], 'incorrect "id" field value'
            assert (
                out["is_starter"] == ref_out["is_starter"]
            ), 'incorrect "is_starter" field value'
            assert (
                out["is_leaver"] == ref_out["is_leaver"]
            ), 'incorrect "is_leaver" field value'
