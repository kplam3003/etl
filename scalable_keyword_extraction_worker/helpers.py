import time
import json
import spacy
import logging
import requests
import numpy as np

from typing import List, Tuple, Dict, Union
from datetime import datetime
from scipy.spatial import distance
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference

import config


class BreakError(Exception):
    def __init__(self, message):
        self.message = message


def _embed_sentences(sentences: List[str]) -> np.array:
    """
    Embed each text sentence into a vector using NLP model.
    Call `encode-sentence` API on Prediction service.
    """
    embeddings = []
    batch_size = 100
    for i in range(0, len(sentences), batch_size):
        sub_sentences = sentences[i : i + batch_size]
        res = requests.post(config.ENCODE_SENTENCE_API, json=sub_sentences)
        assert (
            res.ok
        ), f"Calling API `encode-sentence` with error: {sub_sentences} => {res.reason}"
        vecs = res.json()
        embeddings.extend(vecs)

    return np.array(embeddings)


def _find_shortest_distance(src: np.array, dst: np.array) -> List[Tuple[int, float]]:
    """
    For each vector in `src` matrix, return an index of vector in `dst` matrix that has the shortest cosine distance.
    """
    D = distance.cdist(src, dst, metric="cosine")
    max_ids = np.argmin(D, axis=1, keepdims=True)
    max_ids: List[int] = np.squeeze(max_ids, axis=1).tolist()
    results = [(max_id, D[i][max_id]) for i, max_id in enumerate(max_ids)]
    return results


def load_hra_casestudy_dimension_config(
    case_study_id: int,
    dimension_config: dict,
    run_id: str,
) -> int:
    """
    Check existing of a dimension config using `id` and `version_id` fields.
    Insert a row into `dwh.hra_casestudy_dimension_config` if not existed.
    """
    dimension_config_id = int(dimension_config["id"])
    dimension_config_version = int(dimension_config["version_id"])
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)

    errors = bq_client.insert_rows_json(
        table=config.GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG,
        json_rows=[
            {
                "case_study_id": case_study_id,
                "dimension_config_id": dimension_config_id,
                "dimension_config_name": dimension_config["name"],
                "dimensions": [
                    {
                        "dimension_type": dim["type"],
                        "is_used": dim["is_use_for_analysis"],
                        "is_user_defined": fun["is_user_defined"],
                        "job_function_group": dim["nlp"],
                        "modified_job_function_group": dim["modified"],
                        "job_function": fun["nlp"],
                        "modified_job_function": fun["modified"],
                        "keywords": fun["keywords"],
                    }
                    for dim in dimension_config["dimensions"]
                    for fun in dim["labels"]
                ],
                "nlp_pack": dimension_config["nlp_pack"],
                "version": dimension_config_version,
                "run_id": run_id,
                "created_at": datetime.utcnow().isoformat(),
            }
        ],
    )
    assert len(errors) == 0, json.dumps(errors)

    return dimension_config_version


def _load_experiences(
    case_study_id: int, company_datasource_id: int
) -> Union[Dict[int, str], None]:
    """
    Return (experience ID, title) from `staging.coresignal_employees_experiences` for each experience.
    """
    # Load experience from `staging.coresignal_employees_experiences`
    query = f"""
        SELECT experience_id, title
        FROM `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES}`
        WHERE case_study_id = {case_study_id}
            AND company_datasource_id = {company_datasource_id}
            AND title is not null;
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    experiences = {int(row[0]): str(row[1]) for row in rows}

    return experiences


def _compute_experience_function(
    experiences: Dict[int, str], dimension_config: dict
) -> List[dict]:
    """
    Compute job function using job title.
    Return job function as a dictionary for each job title.
    """
    # Embed job title
    job_titles: List[str] = list(set(experiences.values()))
    job_titles_embeddings = _embed_sentences(sentences=job_titles)

    # Embed job function keyword
    fn_keywords: List[str] = []
    for group in dimension_config["dimensions"]:
        for fun in group["labels"]:
            fn_keywords.extend(fun["keywords"])
    fn_keywords_embeddings = _embed_sentences(sentences=fn_keywords)

    # Find job function for job title
    shortest_distances = _find_shortest_distance(
        src=job_titles_embeddings, dst=fn_keywords_embeddings
    )

    def __get_job_function(keyword: str) -> Tuple[str, str]:
        for group in dimension_config["dimensions"]:
            job_function_group = group["nlp"]
            for fun in group["labels"]:
                if keyword in fun["keywords"]:
                    return (job_function_group, fun["nlp"])
        return (None, None)

    job_title_fun_mapping = {}
    for i, title in enumerate(job_titles):
        shortest_distance = shortest_distances[i]
        kw_idx: int = shortest_distance[0]
        similarity: float = shortest_distance[1]
        most_similar_keyword: str = fn_keywords[kw_idx]
        group, fun = __get_job_function(most_similar_keyword)
        job_title_fun_mapping[title] = {
            "job_function": fun,
            "job_function_group": group,
            "job_function_similarity": similarity,
            "most_similar_keyword": most_similar_keyword,
        }

    experience_function = [
        dict(experience_id=k, **job_title_fun_mapping[val])
        for k, val in experiences.items()
    ]
    return experience_function


def process_hra_casestudy_jobfunction(
    case_study_id: int,
    company_datasource: dict,
    dimension_config: dict,
    run_id: str,
    logger: logging.Logger,
) -> None:
    """
    Extract job function using job title and dimension config.
    Also load dimension config into `dwh.hra_casestudy_dimension_config` if it not there or it's new version.
    """
    company_datasource_id = int(company_datasource["company_datasource_id"])
    dimension_config_id = int(dimension_config["id"])
    dimension_config_version = int(dimension_config["version_id"])

    # Load experience from `staging.coresignal_employees`
    t0 = time.time()
    experiences = _load_experiences(
        case_study_id=case_study_id,
        company_datasource_id=int(company_datasource["company_datasource_id"]),
    )
    if not experiences:
        logger.info("Experience already loaded")
        return None
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Load {len(experiences)} experiences took {t1-t0}s"
    logger.info(msg)
    print(msg)

    # Compute job function for experience
    t0 = time.time()
    experience_function = _compute_experience_function(
        experiences=experiences, dimension_config=dimension_config
    )
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Compute job function took {t1-t0}s"
    logger.info(msg)
    print(msg)

    # Insert data into `dwh.hra_experience_function`
    t0 = time.time()
    for item in experience_function:
        item.update(
            {
                "case_study_id": case_study_id,
                "company_datasource_id": company_datasource_id,
                "dimension_config_id": dimension_config_id,
                "dimension_config_version": dimension_config_version,
                "run_id": run_id,
            }
        )
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    tb_ref = TableReference.from_string(
        table_id=config.GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION,
        default_project=config.GCP_PROJECT_ID,
    )
    table = bq_client.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bq_client.load_table_from_json(
        destination=tb_ref,
        json_rows=experience_function,
        job_config=job_config,
    )
    _ = load_job.result()
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Insert job function took {t1-t0}s"
    logger.info(msg)
    print(msg)

    return None


def _load_education(
    case_study_id: int, company_datasource_id: int
) -> Union[Dict[int, str], None]:
    """
    Return (education ID, subtitle) from `staging.coresignal_employees_education` for each experience.
    """
    # Load education from `staging.coresignal_employees_education`
    query = f"""
        SELECT education_id, subtitle
        FROM `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION}`
        WHERE case_study_id = {case_study_id}
            AND company_datasource_id = {company_datasource_id}
            AND subtitle is not null
            AND LENGTH(TRIM(subtitle)) > 1;
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    education = {int(row[0]): str(row[1]) for row in rows}

    return education


def _preprocess_education_subtitle(subtitle: str) -> str:
    for item in [" - ", "(", ")", "’s"]:
        subtitle = subtitle.replace(item, ", ")
    return subtitle


def _compute_education_degree(
    education: Dict[int, str], dimension_config: dict
) -> List[dict]:
    """
    Compute education degree using subtitle.
    Return job function as a dictionary for each subtitle.
    """
    # Embed subtitle
    nlp = spacy.load("en_core_web_md")
    edu_noun_chunks: Dict[str, List[int]] = {}  # Noun chunk -> Edu IDs
    for edu_id, edu_subtitle in education.items():
        edu_subtitle = _preprocess_education_subtitle(edu_subtitle)
        noun_chunks = set([chunk.lemma_ for chunk in nlp(edu_subtitle).noun_chunks])
        if not noun_chunks:
            # Subtitle only contains edu degree short form (e.g. MBA, PhD)
            noun_chunks = [edu_subtitle]
        for noun in noun_chunks:
            if noun in edu_noun_chunks:
                if edu_id not in edu_noun_chunks[noun]:
                    edu_noun_chunks[noun].append(edu_id)
            else:
                edu_noun_chunks[noun] = [edu_id]
    noun_embeddings = _embed_sentences(sentences=list(edu_noun_chunks.keys()))

    # Embed education degree keywords
    edu_degrees: Dict[str, Tuple[str, int]] = {}  # Degree keyword -> Degree
    for dim in dimension_config["dimensions"]:
        for kw in dim["keywords"]:
            edu_degrees[kw] = (dim["level"], dim["code"])
    degree_keywords = list(edu_degrees.keys())
    kw_embeddings = _embed_sentences(sentences=degree_keywords)

    # Find job function for job title
    shortest_distances = _find_shortest_distance(src=noun_embeddings, dst=kw_embeddings)
    edu_degree_mapping: Dict[int, List[dict]] = {}
    for noun_idx, noun in enumerate(edu_noun_chunks.keys()):
        edu_ids = edu_noun_chunks[noun]
        kw_idx, distance = shortest_distances[noun_idx]
        degree = edu_degrees[degree_keywords[kw_idx]]
        for edu_id in edu_ids:
            noun_degree = dict(
                noun=noun,
                degree_level=degree[0],
                degree_code=degree[1],
                distance=distance,
            )
            if edu_id in edu_degree_mapping:
                edu_degree_mapping[edu_id].append(noun_degree)
            else:
                edu_degree_mapping[edu_id] = [noun_degree]

    # Compose the results
    results = []
    for edu_id, subtitle in education.items():
        # Get the closest degree for each noun chunk in education subtitle
        closest_degrees = sorted(
            edu_degree_mapping[edu_id], key=lambda item: item["distance"]
        )
        edu_degree = {
            "education_id": edu_id,
            "subtitle": subtitle,
            "level": closest_degrees[0]["degree_level"],
            "level_code": closest_degrees[0]["degree_code"],
            "confidence": 1 - closest_degrees[0]["distance"],
        }
        results.append(edu_degree)

    return results


def process_hra_casestudy_education_degree(
    case_study_id: int,
    company_datasource: dict,
    dimension_config: dict,
    run_id: str,
    logger: logging.Logger,
) -> None:
    """
    Extract education degree from subtitle using dimension config.
    Write the output to `dwh.hra_education_degree`.
    """
    company_datasource_id = int(company_datasource["company_datasource_id"])
    dimension_config_id = int(dimension_config["id"])
    dimension_config_version = int(dimension_config["version_id"])

    # Load education
    t0 = time.time()
    education = _load_education(
        case_study_id=case_study_id, company_datasource_id=company_datasource_id
    )
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Load {len(education)} education took {t1-t0}s"
    logger.info(msg)
    print(msg)

    if not education:
        return None

    # Extract education degree
    t0 = time.time()
    edu_degrees = _compute_education_degree(
        education=education, dimension_config=dimension_config
    )
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Compute education degree took {t1-t0}s"
    logger.info(msg)
    print(msg)

    if not edu_degrees:
        return None

    # Insert data into `dwh.hra_education_degree`
    t0 = time.time()
    for item in edu_degrees:
        item.update(
            {
                "case_study_id": case_study_id,
                "company_datasource_id": company_datasource_id,
                "dimension_config_id": dimension_config_id,
                "dimension_config_version": dimension_config_version,
                "run_id": run_id,
            }
        )
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    tb_ref = TableReference.from_string(
        table_id=config.GCP_BQ_TABLE_HRA_EDUCATION_DEGREE,
        default_project=config.GCP_PROJECT_ID,
    )
    table = bq_client.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bq_client.load_table_from_json(
        destination=tb_ref,
        json_rows=edu_degrees,
        job_config=job_config,
    )
    _ = load_job.result()
    t1 = time.time()
    msg = f"(case_study_id={case_study_id!r}, company_datasource_id={company_datasource_id!r}): Insert education degree took {t1-t0}s"
    logger.info(msg)
    print(msg)

    return None
