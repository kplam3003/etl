from typing import List, Tuple, Dict, Optional, Union, FrozenSet
from datetime import datetime, date
from functools import lru_cache
import sys

sys.path.append("../")

from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference
from dateutil import relativedelta

import config
from core import logger

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

# [LEON-143](https://vitruvianpartners.atlassian.net/browse/LEON-143): 5 years from case study creation date
MONTH_LIMIT = 5 * 12  # 5 years


def _parse_date(date_input: Union[None, str, date, datetime]) -> Optional[date]:
    """
    Parse `date_input` into an instance of `datetime` class.
    If `date_input` is a string, it is expected in %Y-%m-%d format.
    """
    if not date_input:
        return None

    date_output = None
    if isinstance(date_input, datetime):
        date_output = date_input.date()
    elif isinstance(date_input, date):
        date_output = date_input
    else:
        try:
            date_output = datetime.strptime(date_input, "%Y-%m-%d").date()
        except ValueError:
            pass

    return date_output


@lru_cache(maxsize=None)
def _count_non_concurrent_days(
    date_range: FrozenSet[Tuple[date, date]], reference_date: date
) -> int:
    """
    Count total non-concurrent days from `date_range`.

    By counting unique dates in all date range upto `reference_date`, we will have the result.
    Not the efficient way but the easiest one.
    """
    t_r = int(
        datetime(
            reference_date.year, reference_date.month, reference_date.day
        ).timestamp()
    )
    dates_in_all_ranges: List[int] = []
    for item in date_range:
        t_0 = int(datetime(item[0].year, item[0].month, item[0].day).timestamp())
        t_1 = int(datetime(item[1].year, item[1].month, item[1].day).timestamp())
        dates_in_range = list(range(t_0, min(t_1, t_r), 86400))
        dates_in_all_ranges.extend(dates_in_range)
        dates_in_all_ranges = list(set(dates_in_all_ranges))

    return len(dates_in_all_ranges)


def _calc_workforce_years(reference_date: date, exps: List[dict]) -> Optional[float]:
    date_range = [
        (
            _parse_date(item.get("date_from")),
            _parse_date(item.get("date_to"))
            if item.get("date_to")
            else datetime.now().date(),
        )
        for item in exps
    ]
    date_range: List[Tuple[date, date]] = [
        item
        for item in date_range
        if (item[0] and item[1] and (item[0] < reference_date))
    ]
    date_range = sorted(date_range, key=lambda item: item[0])

    if not date_range:
        return None

    total_days = _count_non_concurrent_days(
        date_range=frozenset(date_range), reference_date=reference_date
    )

    return round(total_days / 365, 2)


def _calc_company_years(
    reference_date: date, coresignal_company_id: Union[None, int], exps: List[dict]
) -> Optional[float]:
    if not coresignal_company_id:
        return None

    company_exps = [
        item for item in exps if item["coresignal_company_id"] == coresignal_company_id
    ]
    date_range = [
        (
            _parse_date(item.get("date_from")),
            _parse_date(item.get("date_to"))
            if item.get("date_to")
            else datetime.now().date(),
        )
        for item in company_exps
    ]
    date_range: List[Tuple[date, date]] = [
        item
        for item in date_range
        if (item[0] and item[1] and (item[0] < reference_date))
    ]
    date_range = sorted(date_range, key=lambda item: item[0])

    if not date_range:
        return None

    total_days = _count_non_concurrent_days(
        date_range=frozenset(date_range), reference_date=reference_date
    )

    return round(total_days / 365, 2)


def _calc_role_years(
    reference_date: date,
    date_from: Union[Optional[str], datetime],
    date_to: Union[Optional[str], datetime],
) -> Optional[float]:
    d_from = _parse_date(date_from)
    if not d_from:
        return None
    if d_from > reference_date:
        return None

    d_to = _parse_date(date_to)
    if not d_to:
        d_to = datetime.now().date()

    return round((min(d_to, reference_date) - d_from).days / 365, 2)


def _calc_last_education_years(
    reference_date: date, edus: List[dict]
) -> Optional[float]:
    d_to_edus = [_parse_date(item.get("date_to")) for item in edus]
    d_to_edus = [
        item for item in d_to_edus if (item is not None) and (item < reference_date)
    ]
    if not d_to_edus:
        return None

    last_edu_date_to = sorted(d_to_edus)[-1]

    return round((reference_date - last_edu_date_to).days / 365, 2)


def _calc_tenure_values(
    emp_id: int,
    emp_exp_coll: List[dict],
    emp_edu_coll: List[dict],
    ref_date: Union[None, date] = None,
) -> List[dict]:
    employees_tenures: List[dict] = []
    for emp_exp in emp_exp_coll:
        exp_ref_date = ref_date
        role_ref_date = ref_date
        if not ref_date:
            exp_ref_date = _parse_date(emp_exp.get("date_from"))
            role_ref_date = _parse_date(emp_exp.get("date_to"))

        if not exp_ref_date:
            continue

        if not role_ref_date:
            role_ref_date = date.today()

        emp_tenure = {"employee_id": emp_id, "experience_id": emp_exp["experience_id"]}
        emp_tenure["workforce_years"] = _calc_workforce_years(
            reference_date=exp_ref_date, exps=emp_exp_coll
        )
        emp_tenure["company_years"] = _calc_company_years(
            reference_date=exp_ref_date,
            coresignal_company_id=emp_exp["coresignal_company_id"],
            exps=emp_exp_coll,
        )
        emp_tenure["role_years"] = _calc_role_years(
            reference_date=role_ref_date,
            date_from=emp_exp.get("date_from"),
            date_to=emp_exp.get("date_to"),
        )
        emp_tenure["last_education_years"] = _calc_last_education_years(
            reference_date=exp_ref_date, edus=emp_edu_coll
        )
        employees_tenures.append(emp_tenure)

    return employees_tenures


def _calc_tenure_monthly_values(
    emp_id: int, emp_exp_coll: List[dict], emp_edu_coll: List[dict]
) -> List[dict]:
    monthly_tenures = []
    ref_dates = [
        date.today() - relativedelta.relativedelta(months=(1 * nth_month))
        for nth_month in range(MONTH_LIMIT)
    ]
    for ref_date in ref_dates:
        month_tenures = _calc_tenure_values(
            emp_id=emp_id,
            emp_exp_coll=emp_exp_coll,
            emp_edu_coll=emp_edu_coll,
            ref_date=ref_date,
        )
        month_tenures = [
            dict(**item, reference_date=ref_date.isoformat()) for item in month_tenures
        ]
        monthly_tenures.extend(month_tenures)

    return monthly_tenures


def _get_coresignal_employees_experiences(
    case_study_id: int, company_datasource_id: int
) -> Dict[int, List[Dict]]:
    """
    Return a `dict` with key is `coresignal_employee_id`, value is a `list` of experience.
    """
    query = f"""
        select coresignal_employee_id, 
            array_agg(struct(experience_id, title, coresignal_company_id, date_from, date_to)) as experiences
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES}`
        where case_study_id = {case_study_id} and company_datasource_id = {company_datasource_id}
        group by coresignal_employee_id;
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    exps = {row["coresignal_employee_id"]: row["experiences"] for row in rows}

    return exps


def _get_coresignal_employees_education(
    case_study_id: int, company_datasource_id: int
) -> Dict[int, List[Dict]]:
    """
    Return a `dict` with key is `coresignal_employee_id`, value is a `list` of education.
    """
    query = f"""
        select coresignal_employee_id, 
            array_agg(struct(education_id, date_from, date_to)) as education
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION}`
        where case_study_id = {case_study_id} and company_datasource_id = {company_datasource_id}
        group by coresignal_employee_id;
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    exps = {row["coresignal_employee_id"]: row["education"] for row in rows}

    return exps


def _check_coresignal_employees_tenure(
    case_study_id: int, company_datasource_id: int
) -> bool:
    """
    Check if employee tenure data already loaded into DWH.
    """
    query = f"""
        select count(*) as nrows
        from {config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE}
        where case_study_id = {case_study_id} and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()

    for row in rows:
        if int(row["nrows"]) != 0:
            return True
    return False


def insert_coresignal_employees_tenure(
    case_study_id: int,
    case_payload: dict,
):
    """
    Calculate tenure fields for experience dataset and monthly dataset for all experiences in a case study.
    Input tables: staging.coresignal_employees_experiences, staging.coresignal_employees_education
    Output tables: dwh.hra_employees_tenure, dwh.hra_employees_tenure_monthly
    """
    for cds in case_payload["company_datasources"]:
        # Get employee experience & education from Staging
        cds_id: int = cds["company_datasource_id"]
        loaded = _check_coresignal_employees_tenure(
            case_study_id=case_study_id, company_datasource_id=cds_id
        )

        if loaded:
            continue

        experiences = _get_coresignal_employees_experiences(
            case_study_id=case_study_id, company_datasource_id=cds_id
        )
        education = _get_coresignal_employees_education(
            case_study_id=case_study_id, company_datasource_id=cds_id
        )
        employees = {
            emp_id: {
                "experiences": experiences[emp_id],
                "education": education[emp_id] if emp_id in education else [],
            }
            for emp_id in experiences
        }

        # Prepare data before insert
        employees_tenures = []
        employees_tenures_monthly = []
        for emp_id in employees:
            emp_edu_coll: List[dict] = employees[emp_id]["education"]
            emp_exp_coll: List[dict] = employees[emp_id]["experiences"]

            emp_exp_tenures = _calc_tenure_values(
                emp_id=emp_id, emp_exp_coll=emp_exp_coll, emp_edu_coll=emp_edu_coll
            )
            emp_exp_tenures_monthly = _calc_tenure_monthly_values(
                emp_id=emp_id, emp_exp_coll=emp_exp_coll, emp_edu_coll=emp_edu_coll
            )

            employees_tenures.extend(emp_exp_tenures)
            employees_tenures_monthly.extend(emp_exp_tenures_monthly)

        # Insert into `hra_employees_tenure`
        employees_tenures = [
            dict(**item, case_study_id=case_study_id, company_datasource_id=cds_id)
            for item in employees_tenures
        ]
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        tb_ref = TableReference.from_string(
            table_id=config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE,
            default_project=config.GCP_PROJECT_ID,
        )
        table = bq_client.get_table(tb_ref)
        job_config = bigquery.LoadJobConfig(schema=table.schema)
        load_job = bq_client.load_table_from_json(
            destination=tb_ref,
            json_rows=employees_tenures,
            job_config=job_config,
        )
        _ = load_job.result()

        # Insert into `hra_employees_tenure_monthly`
        employees_tenures_monthly = [
            dict(**item, case_study_id=case_study_id, company_datasource_id=cds_id)
            for item in employees_tenures_monthly
        ]
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        tb_ref = TableReference.from_string(
            table_id=config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE_MONTHLY,
            default_project=config.GCP_PROJECT_ID,
        )
        table = bq_client.get_table(tb_ref)
        job_config = bigquery.LoadJobConfig(schema=table.schema)
        load_job = bq_client.load_table_from_json(
            destination=tb_ref,
            json_rows=employees_tenures_monthly,
            job_config=job_config,
        )
        _ = load_job.result()

    return None
