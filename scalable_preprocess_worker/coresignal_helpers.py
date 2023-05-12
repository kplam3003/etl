from typing import Any, Dict, List, Union
import datetime

import requests
from dateparser import parse

import config


def find_previous_next_company(
    member_experiences: List[Dict[str, Any]],
    leo_company_id: int,
    ref_coresignal_company_ids: List[int],
) -> List[Dict[str, Any]]:

    # handle cases where date_from is None or does not exist
    exp_data_error = [m for m in member_experiences if not m.get("date_from")]
    for d in exp_data_error:
        d["previous_company_name"] = None
        d["previous_coresignal_company_id"] = None
        d["previous_experience_id"] = None
        d["next_company_name"] = None
        d["next_coresignal_company_id"] = None
        d["next_experience_id"] = None
        # add leo company_id
        if d["company_id"] in ref_coresignal_company_ids:
            current_leo_company_ids = set(d.get("leo_company_ids", []))
            current_leo_company_ids.add(leo_company_id)
            d["leo_company_ids"] = list(current_leo_company_ids)

    # handle normal cases
    # sort by date_from
    exp_data = [m for m in member_experiences if m.get("date_from")]
    exp_data = sorted(exp_data, key=lambda x: x["date_from"])
    for i in range(len(exp_data)):
        current_exp = exp_data[i]
        # add leo company_id
        if current_exp["company_id"] in ref_coresignal_company_ids:
            current_leo_company_ids = set(current_exp.get("leo_company_ids", []))
            current_leo_company_ids.add(leo_company_id)
            current_exp["leo_company_ids"] = list(current_leo_company_ids)

        if len(exp_data) == 1:
            current_exp["previous_company_name"] = None
            current_exp["previous_coresignal_company_id"] = None
            current_exp["previous_experience_id"] = None
            current_exp["next_company_name"] = None
            current_exp["next_coresignal_company_id"] = None
            current_exp["next_experience_id"] = None

        elif i == 0:
            current_exp["previous_company_name"] = None
            current_exp["previous_coresignal_company_id"] = None
            current_exp["previous_experience_id"] = None
            current_exp["next_company_name"] = exp_data[i + 1]["company_name"]
            current_exp["next_coresignal_company_id"] = exp_data[i + 1]["company_id"]
            current_exp["next_experience_id"] = exp_data[i + 1]["id"]

        elif i == len(exp_data) - 1:
            current_exp["previous_company_name"] = exp_data[i - 1]["company_name"]
            current_exp["previous_coresignal_company_id"] = exp_data[i - 1][
                "company_id"
            ]
            current_exp["previous_experience_id"] = exp_data[i - 1]["id"]
            current_exp["next_company_name"] = None
            current_exp["next_coresignal_company_id"] = None
            current_exp["next_experience_id"] = None

        else:
            current_exp["previous_company_name"] = exp_data[i - 1]["company_name"]
            current_exp["previous_coresignal_company_id"] = exp_data[i - 1][
                "company_id"
            ]
            current_exp["previous_experience_id"] = exp_data[i - 1]["id"]
            current_exp["next_company_name"] = exp_data[i + 1]["company_name"]
            current_exp["next_coresignal_company_id"] = exp_data[i + 1]["company_id"]
            current_exp["next_experience_id"] = exp_data[i + 1]["id"]

    exp_data.extend(exp_data_error)
    return exp_data


def check_starter_leaver(
    member_experiences: List[Dict[str, Any]],
    leo_company_id: int,
    ref_coresignal_company_ids: List[int],
) -> List[Dict[str, Any]]:
    exp_data = member_experiences.copy()
    for i, current_exp in enumerate(exp_data):
        current_coresignal_company_id = current_exp["company_id"]
        current_leo_company_id = (
            leo_company_id
            if current_coresignal_company_id in ref_coresignal_company_ids
            else 0
        )
        if len(exp_data) == 1:
            current_exp["is_starter"] = True
            current_exp["is_leaver"] = current_exp.get("end_date") is not None
            break

        is_first_exp = i == 0
        is_last_exp = i == len(exp_data) - 1
        # LEON-130
        # assign ids
        previous_coresignal_company_id = current_exp["previous_coresignal_company_id"]
        previous_leo_company_id = (
            leo_company_id
            if previous_coresignal_company_id in ref_coresignal_company_ids
            # NOTE: this is -1 and the next is -2 to avoid both being the same
            # when both is not available
            else -1
        )
        next_coresignal_company_id = current_exp["next_coresignal_company_id"]
        next_leo_company_id = (
            leo_company_id
            if next_coresignal_company_id in ref_coresignal_company_ids
            else -2
        )

        # dates
        previous_end_date = (
            None if is_first_exp else parse(str(exp_data[i - 1]["date_to"]))
        )
        current_start_date = parse(str(current_exp["date_from"]))
        current_end_date = parse(str(current_exp["date_to"]))
        next_start_date = (
            None if is_last_exp else parse(str(exp_data[i + 1]["date_from"]))
        )

        # establish rules
        # general
        has_end_date = current_exp.get("date_to") is not None
        # previous/starter
        has_same_previous_coresignal_id = (
            current_coresignal_company_id == previous_coresignal_company_id
        )
        has_same_previous_leo_id = current_leo_company_id == previous_leo_company_id
        has_previous_end_within_60_days = check_day_diff_within(
            previous_end_date, current_start_date, 60
        )
        current_exp["is_starter"] = (
            True
            if is_first_exp
            else not (
                (has_same_previous_coresignal_id or has_same_previous_leo_id)
                and has_previous_end_within_60_days
            )
        )

        # next/leaver
        has_same_next_coresignal_id = (
            current_coresignal_company_id == next_coresignal_company_id
        )
        has_same_next_leo_id = current_leo_company_id == next_leo_company_id
        has_next_end_within_60_days = check_day_diff_within(
            current_end_date, next_start_date, 60
        )
        current_exp["is_leaver"] = (
            False
            if (is_last_exp or not has_end_date)
            else not (
                (has_same_next_coresignal_id or has_same_next_leo_id)
                and has_next_end_within_60_days
            )
        )

    return exp_data


def encode_job_title(
    member_experiences: List[Dict[str, Any]]
) -> Dict[str, List[float]]:
    """
    Encode job title into vector space using NLP model.
    """
    job_titles = [str(item.get("title", "")) for item in member_experiences]
    experience_ids = [item["id"] for item in member_experiences]
    if not job_titles:
        return None
    res = requests.post(config.ENCODE_SENTENCE_API, json=job_titles)
    embeddings: List[List[float]] = res.json()
    return {exp_id: embeddings[idx] for idx, exp_id in enumerate(experience_ids)}


def check_day_diff_within(
    date_from: Union[datetime.datetime, datetime.date, None],
    date_to: Union[datetime.datetime, datetime.date, None],
    days_diff: Union[int, float] = None,
) -> bool:
    try:
        diff = date_to - date_from
        return diff <= datetime.timedelta(days=days_diff)

    except TypeError:
        return False
