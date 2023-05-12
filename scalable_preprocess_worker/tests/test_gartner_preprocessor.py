import os
from datetime import datetime
from copy import deepcopy
import sys

sys.path.append("../")
sys.path.append("../../")

import unittest

from app import transform_gartner


# set up test case
class GartnerPreprocessorTest(unittest.TestCase):
    def setUp(self):
        self.payload = {"batch": {"source_name": "Gartner", "company_id": 2}}

        self.formattedReviewDate = "2021-01-01"
        self.reviewId = "abc123"
        self.reviewRating = 5
        self.reviewer = r'{"fullName": "HH"}'

        self.base_item = {
            "formattedReviewDate": self.formattedReviewDate,
            "reviewer": self.reviewer,
            "reviewId": self.reviewId,
            "reviewRating": self.reviewRating,
            "reviewSummary": "this is summary",
        }
        self.full_review_dict = {
            "review-summary": "this is summary",
            "lessonlearned-like-most": "this is pros",
            "lessonlearned-dislike-most": "this is cons",
            "business-problem-solved": "this is problems solved",
            "lessonlearned-advice": "this is recommendation or advice",
            "why-purchase-s24": "this is the reason for switching or purchasing number 1",
            "why-purchase-s24-other": "this is the reason for switching or purchasing number 2",
        }

        self.technical_types = (
            "overall",
            "pros",
            "cons",
            "problems_solved",
            "recommendations",
            "switch_reasons",
            "all",
        )

    def _construct_test_result(self, review_dict):
        test_item = self.base_item.copy()
        test_item.update(review_dict)
        result = transform_gartner(self.payload, test_item)

        return result

    def _sanity_tests(self, results):
        for res in results:
            # required fields
            self.assertIn("review", res)
            self.assertIn("review_id", res)
            self.assertIn("parent_review_id", res)
            self.assertIn("technical_type", res)
            self.assertIn("__review_date__", res)

            # technical type values
            self.assertIn(
                res["technical_type"],
                self.technical_types,
                f"invalid technical type: {res['technical_type']}",
            )

    def test_full_review_fields(self):
        # construct preprocessing results for testing
        results = self._construct_test_result(self.full_review_dict)

        # sanity tests
        self._sanity_tests(results=results)

        # general tests
        self.assertEqual(len(results), 6, "incorrect number of items returned")

    def test_1_missing_review_field(self):
        # construct preprocessing results for testing
        for key in self.full_review_dict.keys():
            _temp_review_dict = deepcopy(self.full_review_dict)
            del _temp_review_dict[key]
            results = self._construct_test_result(_temp_review_dict)

            # sanity tests
            self._sanity_tests(results=results)

            # test for number of items
            if key in ("why-purchase-s24", "why-purchase-s24-other"):
                self.assertEqual(
                    len(results),
                    6,
                    f"incorrect number of items returned for missing field {key}",
                )
            else:
                self.assertEqual(
                    len(results),
                    5,
                    f"incorrect number of items returned for missing field {key}",
                )

    def test_1_empty_review_field(self):
        # construct preprocessing results for testing
        for key in self.full_review_dict.keys():
            _temp_review_dict = deepcopy(self.full_review_dict)
            _temp_review_dict[key] = ""
            results = self._construct_test_result(_temp_review_dict)

            # sanity tests
            self._sanity_tests(results=results)

            # test for number of items
            if key in ("why-purchase-s24", "why-purchase-s24-other"):
                self.assertEqual(
                    len(results),
                    6,
                    f"incorrect number of items returned for empty field {key}",
                )
            else:
                self.assertEqual(
                    len(results),
                    5,
                    f"incorrect number of items returned for empty field {key}",
                )

    def test_missing_why_review_fields(self):
        # why-purchase-s24
        # construct preprocessing results for testing
        _temp_review_dict = deepcopy(self.full_review_dict)
        del _temp_review_dict["why-purchase-s24"]
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 6, "incorrect number of items returned")

        # why-purchase-s24-other
        # construct preprocessing results for testing
        _temp_review_dict = deepcopy(self.full_review_dict)
        del _temp_review_dict["why-purchase-s24-other"]
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 6, "incorrect number of items returned")

        # both
        _temp_review_dict = deepcopy(self.full_review_dict)
        del _temp_review_dict["why-purchase-s24"]
        del _temp_review_dict["why-purchase-s24-other"]
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 5, "incorrect number of items returned")

    def test_empty_why_review_fields(self):
        # why-purchase-s24
        # construct preprocessing results for testing
        _temp_review_dict = deepcopy(self.full_review_dict)
        _temp_review_dict["why-purchase-s24"] = ""
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 6, "incorrect number of items returned")

        # why-purchase-s24-other
        # construct preprocessing results for testing
        _temp_review_dict = deepcopy(self.full_review_dict)
        _temp_review_dict["why-purchase-s24-other"] = ""
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 6, "incorrect number of items returned")

        # both
        _temp_review_dict = deepcopy(self.full_review_dict)
        _temp_review_dict["why-purchase-s24"] = ""
        _temp_review_dict["why-purchase-s24-other"] = ""
        results = self._construct_test_result(_temp_review_dict)

        # sanity tests
        self._sanity_tests(results=results)
        self.assertEqual(len(results), 5, "incorrect number of items returned")


if __name__ == "__main__":
    unittest.main(verbosity=2)
