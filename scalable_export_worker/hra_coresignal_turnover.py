from datetime import date
from typing import List, Tuple
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference

import config


BACK_YEARS = 10


def _check_coresignal_employees_bucket(case_study_id: int, company_id: int) -> bool:
    """Check if any row in `dwh.hra_company_employees_bucket` table for given `case_study_id` and `company_id`"""
    query = f"""
        select count(*) as nrows
        from {config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET}
        where case_study_id = {case_study_id} and company_id = {company_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()

    for row in rows:
        if int(row["nrows"]) != 0:
            return True
    return False


def _get_coresignal_company_ids(
    case_study_id: int, company_datasource_ids: List[int]
) -> List[int]:
    query = f"""
        select distinct coresignal_company_id
        from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}`
        where case_study_id = {case_study_id} and company_datasource_id	in ({",".join([str(item) for item in company_datasource_ids])})
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()

    return [int(row[0]) for row in rows]


def _load_experiences(
    case_study_id: int, company_datasource_ids: List[int]
) -> List[dict]:
    """
    Load experience from `staging.coresignal_employees_experiences`.
    Using `staging.coresignal_company_datasource` table to map `company_datasource_id` to `coresignal_company_id`.
    Then use `coresignal_company_id` to get experience
    """
    query = f"""
        select coresignal_employee_id, coresignal_company_id, experience_id, date_from,
            IFNULL(date_to, CURRENT_DATE()) as date_to
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES}`
        where case_study_id = {case_study_id}
        and coresignal_company_id in (
            select distinct coresignal_company_id
            from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}`
            where case_study_id = {case_study_id} and company_datasource_id	in ({",".join([str(item) for item in company_datasource_ids])})
        )
        and date_from is not null;
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()

    return [{item[0]: item[1] for item in row.items()} for row in rows]


def _aggregate_employee_experience(
    experiences: List[dict],
    coresignal_company_ids: List[int],
    time_key: date,
) -> Tuple[List[int], List[int]]:
    filtered_experiences = [
        item
        for item in experiences
        if (
            (int(item["coresignal_company_id"]) in coresignal_company_ids)
            and (item["date_from"] < time_key and item["date_to"] > time_key)
        )
    ]
    filtered_emp_ids = [
        int(item["coresignal_employee_id"]) for item in filtered_experiences
    ]
    filtered_emp_ids = list(set(filtered_emp_ids))
    filtered_exp_ids = [int(item["experience_id"]) for item in filtered_experiences]

    return (filtered_emp_ids, filtered_exp_ids)


def insert_company_employees_bucket(
    case_study_id: int,
    case_payload: dict,
) -> None:
    """
    Input tables: staging.coresignal_company_datasource, staging.coresignal_employees_experiences
    Output tables: dwh.hra_company_employees_bucket
    """
    cds: List[dict] = case_payload["company_datasources"]
    company_ids = [int(item["company_id"]) for item in cds]
    company_ids = set(company_ids)
    for c_id in company_ids:
        company_rows = []

        # Check if data is existing
        cds_ids = [
            int(item["company_datasource_id"])
            for item in cds
            if int(item["company_id"]) == c_id
        ]
        loaded = _check_coresignal_employees_bucket(
            case_study_id=case_study_id, company_id=c_id
        )

        if loaded:
            continue

        # Load experience
        experiences = _load_experiences(
            case_study_id=case_study_id, company_datasource_ids=cds_ids
        )

        # Aggregate employee ID and experience ID for each year
        current_year = date.today().year
        time_keys = [
            date(year=item, month=1, day=1)
            for item in range(current_year - BACK_YEARS, current_year + 1)
        ]
        coresignal_company_ids = _get_coresignal_company_ids(
            case_study_id=case_study_id, company_datasource_ids=cds_ids
        )
        for time_key in time_keys:
            emp_ids, exp_ids = _aggregate_employee_experience(
                experiences=experiences,
                coresignal_company_ids=coresignal_company_ids,
                time_key=time_key,
            )
            company_rows.append(
                {
                    "case_study_id": case_study_id,
                    "company_id": c_id,
                    "time_key": time_key.isoformat(),
                    "employees": emp_ids,
                    "experiences": exp_ids,
                }
            )

        # Insert into Bigquery
        bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
        tb_ref = TableReference.from_string(
            table_id=config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET,
            default_project=config.GCP_PROJECT_ID,
        )
        table = bq_client.get_table(tb_ref)
        job_config = bigquery.LoadJobConfig(schema=table.schema)
        load_job = bq_client.load_table_from_json(
            destination=tb_ref,
            json_rows=company_rows,
            job_config=job_config,
        )
        _ = load_job.result()

    return None


def build_query_company(case_study_id: int, case_study_name: str):
    query = f"""
        insert into `{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}`(
            case_study_id, case_study_name, company_id, company_name, time_key, category_type, turnover_rate, num_employees_start, num_employees_end, num_starters, num_leavers
        )
        with tmp as (
            with cs_data as (
                select case_study_id, company_id, time_key, employees
                from `{config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET}`
                where case_study_id = {case_study_id} and ARRAY_LENGTH(employees) != 0
            )
            select d0.case_study_id,
                '{case_study_name}' as case_study_name,
                d0.company_id,
                (
                    select distinct company_name 
                    from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}` 
                    where case_study_id = d0.case_study_id and company_id = d0.company_id
                ) as company_name,
                d0.time_key,
                'company' as category_type,
                null as category_value,
                (
                    select count(distinct employee_id) 
                    from unnest(d0.employees) employee_id
                ) as num_employees_start,
                (
                    select count(distinct employee_id) 
                    from unnest(d1.employees) employee_id
                ) as num_employees_end,
                (
                    select count(distinct employee_id1)
                    from unnest(d1.employees) employee_id1
                    where employee_id1 not in (select * from unnest(d0.employees))
                ) as num_starters,
                (
                    select count(distinct employee_id0)
                    from unnest(d0.employees) employee_id0
                    where employee_id0 not in (select * from unnest(d1.employees))
                ) as num_leavers
            from cs_data as d0
                inner join cs_data as d1
                on d0.company_id = d1.company_id 
                and d0.time_key = date_sub(d1.time_key, INTERVAL 1 YEAR)
        )
        select case_study_id,
            case_study_name,
            company_id,
            company_name,
            time_key,
            category_type,
            (
                case num_employees_start+num_employees_end != 0
                    when true then num_leavers/((num_employees_start+num_employees_end)/2)
                    else null
                end
            ) as turnover_rate,
            num_employees_start,
            num_employees_end,
            num_starters,
            num_leavers
        from tmp
        order by company_id, time_key;
    """
    return query


def build_query_company_job_function(
    case_study_id: int, case_study_name: str, run_id: str
):
    query = f"""
        insert into `{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}`(
            case_study_id, case_study_name, company_id, company_name, time_key, category_type, category_value, turnover_rate, num_employees_start, num_employees_end, num_starters, num_leavers
        )
        with tmp as (
            with job_fun as (
                with cs_data as (
                    select case_study_id, company_id, time_key, experiences
                    from `{config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET}`
                    where case_study_id = {case_study_id} and ARRAY_LENGTH(experiences) != 0
                )
                select cs_data.case_study_id, cs_data.company_id, time_key, hef.job_function, ARRAY_AGG(DISTINCT cee.coresignal_employee_id) as employees
                from cs_data, unnest(experiences) exp_id
                    inner join `{config.GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION}` hef
                    on exp_id = hef.experience_id
                    inner join `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES}` cee
                    on hef.experience_id = cee.experience_id
                where hef.case_study_id = {case_study_id}
                    and hef.run_id = '{run_id}'
                    and cee.case_study_id = {case_study_id}
                group by cs_data.case_study_id, cs_data.company_id, time_key, hef.job_function
            )
            select d0.case_study_id,
                '{case_study_name}' as case_study_name,
                d0.company_id,
                (
                    select distinct company_name 
                    from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}` 
                    where case_study_id = d0.case_study_id and company_id = d0.company_id
                ) as company_name,
                d0.time_key,
                'job_function' as category_type,
                d0.job_function as category_value,
                (
                    select count(distinct employee_id) 
                    from unnest(d0.employees) employee_id
                ) as num_employees_start,
                (
                    select count(distinct employee_id) 
                    from unnest(d1.employees) employee_id
                ) as num_employees_end,
                (
                    select count(distinct employee_id1)
                    from unnest(d1.employees) employee_id1
                    where employee_id1 not in (select * from unnest(d0.employees))
                ) as num_starters,
                (
                    select count(distinct employee_id0)
                    from unnest(d0.employees) employee_id0
                    where employee_id0 not in (select * from unnest(d1.employees))
                ) as num_leavers
            from job_fun as d0
                inner join job_fun as d1
                on d0.company_id = d1.company_id
                and d0.job_function = d1.job_function
                and d0.time_key = date_sub(d1.time_key, INTERVAL 1 YEAR)
        )
        select case_study_id,
            case_study_name,
            company_id,
            company_name,
            time_key,
            category_type,
            category_value,
            (
                case num_employees_start+num_employees_end != 0
                    when true then num_leavers/((num_employees_start+num_employees_end)/2)
                    else null
                end
            ) as turnover_rate,
            num_employees_start,
            num_employees_end,
            num_starters,
            num_leavers
        from tmp
        order by company_id, category_value, time_key;
    """
    return query


def build_query_company_tenure_bucket(case_study_id: int, case_study_name: str):
    query = f"""
        insert into `{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}`(
            case_study_id, case_study_name, company_id, company_name, time_key, category_type, category_value, turnover_rate, num_employees_start, num_employees_end, num_starters, num_leavers
        )
        with tmp as (
            with tenure as (
                with bucket as(
                    with cs_data as (
                        select case_study_id, company_id, time_key, experiences
                        from `{config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET}`
                        where case_study_id = {case_study_id} and ARRAY_LENGTH(experiences) != 0
                    )
                    select cs_data.case_study_id, cs_data.company_id, time_key,
                        (
                            case
                                when hetm.company_years <= 1 then '1: 0-1 years'
                                when hetm.company_years <= 2 then '2: 1-2 years'
                                when hetm.company_years <= 5 then '3: 2-5 years'
                                when hetm.company_years <= 10 then '4: 5-10 years'
                                else '5: 10+ years'
                            end
                        ) as tenure_bucket,
                        hetm.employee_id
                    from cs_data, unnest(experiences) exp_id
                        inner join `{config.GCP_BQ_TABLE_HRA_EMPLOYEES_TENURE_MONTHLY}` hetm
                        on exp_id = hetm.experience_id
                            and EXTRACT(YEAR FROM time_key) = EXTRACT(YEAR FROM hetm.reference_date)
                            and EXTRACT(MONTH FROM time_key) = EXTRACT(MONTH FROM hetm.reference_date)
                    where hetm.case_study_id = {case_study_id}
                )
                select case_study_id, company_id, time_key, tenure_bucket, ARRAY_AGG(DISTINCT employee_id) as employees
                from bucket
                group by case_study_id, company_id, time_key, tenure_bucket
            )
            select d0.case_study_id,
                '{case_study_name}' as case_study_name,
                d0.company_id,
                (
                    select distinct company_name 
                    from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}` 
                    where case_study_id = d0.case_study_id and company_id = d0.company_id
                ) as company_name,
                d0.time_key,
                'tenure_bucket' as category_type,
                d0.tenure_bucket as category_value,
                (
                    select count(distinct employee_id) 
                    from unnest(d0.employees) employee_id
                ) as num_employees_start,
                (
                    select count(distinct employee_id) 
                    from unnest(d1.employees) employee_id
                ) as num_employees_end,
                (
                    select count(distinct employee_id1)
                    from unnest(d1.employees) employee_id1
                    where employee_id1 not in (select * from unnest(d0.employees))
                ) as num_starters,
                (
                    select count(distinct employee_id0)
                    from unnest(d0.employees) employee_id0
                    where employee_id0 not in (select * from unnest(d1.employees))
                ) as num_leavers
            from tenure as d0
                inner join tenure as d1
                on d0.company_id = d1.company_id
                and d0.tenure_bucket = d1.tenure_bucket
                and d0.time_key = date_sub(d1.time_key, INTERVAL 1 YEAR)
        )
        select case_study_id,
            case_study_name,
            company_id,
            company_name,
            time_key,
            category_type,
            cast(category_value as string) as category_value,
            (
                case num_employees_start+num_employees_end != 0
                    when true then num_leavers/((num_employees_start+num_employees_end)/2)
                    else null
                end
            ) as turnover_rate,
            num_employees_start,
            num_employees_end,
            num_starters,
            num_leavers
        from tmp
        order by company_id, category_value, time_key;
    """
    return query


def build_query_company_country(case_study_id: int, case_study_name: str):
    query = f"""
        insert into `{config.GCP_BQ_TABLE_HRA_SUMMARY_TABLE_TURNOVER_PREFIX}_{case_study_id}`(
            case_study_id, case_study_name, company_id, company_name, time_key, category_type, category_value, turnover_rate, num_employees_start, num_employees_end, num_starters, num_leavers
        )
        with tmp as (
            with country as (
                with cs_data as (
                    select case_study_id, company_id, time_key, employees
                    from `{config.GCP_BQ_TABLE_HRA_COMPANY_EMPLOYEES_BUCKET}`
                    where case_study_id = {case_study_id} and ARRAY_LENGTH(employees) != 0
                )
                select cs_data.case_study_id, cs_data.company_id, time_key, ce.country, ARRAY_AGG(DISTINCT ce.coresignal_employee_id) as employees
                from cs_data, unnest(employees) emp_id
                    inner join `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES}` ce
                    on emp_id = ce.coresignal_employee_id
                where ce.case_study_id = {case_study_id}
                group by cs_data.case_study_id, cs_data.company_id, time_key, ce.country
            )
            select d0.case_study_id,
                '{case_study_name}' as case_study_name,
                d0.company_id,
                (
                    select distinct company_name 
                    from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}` 
                    where case_study_id = d0.case_study_id and company_id = d0.company_id
                ) as company_name,
                d0.time_key,
                'country' as category_type,
                d0.country as category_value,
                (
                    select count(distinct employee_id) 
                    from unnest(d0.employees) employee_id
                ) as num_employees_start,
                (
                    select count(distinct employee_id) 
                    from unnest(d1.employees) employee_id
                ) as num_employees_end,
                (
                    select count(distinct employee_id1)
                    from unnest(d1.employees) employee_id1
                    where employee_id1 not in (select * from unnest(d0.employees))
                ) as num_starters,
                (
                    select count(distinct employee_id0)
                    from unnest(d0.employees) employee_id0
                    where employee_id0 not in (select * from unnest(d1.employees))
                ) as num_leavers
            from country as d0
                inner join country as d1
                on d0.company_id = d1.company_id
                and d0.country = d1.country
                and d0.time_key = date_sub(d1.time_key, INTERVAL 1 YEAR)
        )
        select case_study_id,
            case_study_name,
            company_id,
            company_name,
            time_key,
            category_type,
            category_value,
            (
                case num_employees_start+num_employees_end != 0
                    when true then num_leavers/((num_employees_start+num_employees_end)/2)
                    else null
                end
            ) as turnover_rate,
            num_employees_start,
            num_employees_end,
            num_starters,
            num_leavers
        from tmp
        order by company_id, category_value, time_key;
    """
    return query
