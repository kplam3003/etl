# functions for custom dimension extraction
import logging
import re
from datetime import datetime
from typing import Any, List, Dict, Union

from google.cloud import bigquery

    
def get_user_defined_dimension_labels(
    payload_dimensions: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Extract and transform user defined dimensions from payload
    to a usable format
    """
    user_defined_dimensions = []
    for dimension in payload_dimensions:
        for label in dimension['labels']:
            if label.get('is_user_defined', False):
                user_defined_dimension = {
                    'dimension': dimension['nlp'],
                    'label': label['nlp'],
                    'keywords': label['keywords']
                }
                user_defined_dimensions.append(user_defined_dimension)
    
    return user_defined_dimensions


def _extract_custom_dimension_from_keyword(
    review_text: str,
    dimension_name: str,
    label_name: str,
    keywords: List[str]
) -> Union[Dict[str, Any]]:
    """
    Extract custom dimensions from text for a given 
    pair of dimension-label and its keywords
    keys for output items are
    ('code', 'dimension', 'label', 'terms', 'abs_relevance', 'rel_relevance')
    """
    found_keywords = []
    for kw in keywords:
        sanitized_kw = re.escape(kw)
        pattern = fr'\b{sanitized_kw}\b'
        found_keywords.extend(re.findall(
            pattern, 
            review_text, 
            flags=re.IGNORECASE
        ))
        
    if found_keywords:
        return {
            'code': f'{dimension_name}>{label_name}',
            'dimension': dimension_name,
            'label': label_name,
            'terms': '\t'.join(found_keywords),
            'relevance': 1 + len(found_keywords),
            'rel_relevance': 100,
        }
    else:
        return []
            

def _calculate_custom_dimension_score(
    custom_dimension_item: Dict[str, Any],
    mc_polarities: List[str],
    polarity_mapping: Dict[str, int]
) -> List[Dict[str, Any]]:
    """
    Calculate polarity based on:
    - MeaningCloud NLP results
    - Polarity Mappings
    """
    new_custom_dimension_item = custom_dimension_item.copy()
    # in case when MC return no result at all
    if not mc_polarities:
        # new_custom_dimension_item['sentiment_score'] = None
        new_custom_dimension_item['polarity'] = 'NONE'
        return new_custom_dimension_item
    
    # MC has results
    # NONE polarity does not count into semtiment aggregation
    all_sentiment_scores = [
        polarity_mapping[pol]
        for pol in mc_polarities
        if pol not in ('NONE', 'NULL')
    ]
    # all MC polarity is NONE => custom polarity is also NONE
    if not all_sentiment_scores:
        # new_custom_dimension_item['sentiment_score'] = None
        new_custom_dimension_item['polarity'] = 'NONE'
        return new_custom_dimension_item

    # custom sentiment score calculation and mapping
    sentiment_score = sum(all_sentiment_scores) / len(all_sentiment_scores)
    # new_custom_dimension_item['sentiment_score'] = sentiment_score 
    # map to polarity
    # thresholds
    p_plus_thres  = (polarity_mapping['P'] + polarity_mapping['P+']) / 2
    p_thres  = (polarity_mapping['NEU'] + polarity_mapping['P']) / 2
    n_thres  = (polarity_mapping['NEU'] + polarity_mapping['N']) / 2
    n_plus_thres  = (polarity_mapping['N'] + polarity_mapping['N+']) / 2
    # mapping
    if sentiment_score >= p_plus_thres:
        new_custom_dimension_item['polarity'] = 'P+'
    elif p_plus_thres > sentiment_score >= p_thres:
        new_custom_dimension_item['polarity'] = 'P'
    elif p_thres > sentiment_score > n_thres:
        new_custom_dimension_item['polarity'] = 'NEU'
    elif n_thres >= sentiment_score > n_plus_thres:
        new_custom_dimension_item['polarity'] = 'N'
    elif n_plus_thres >= sentiment_score:
        new_custom_dimension_item['polarity'] = 'P'
    else:
        new_custom_dimension_item['polarity'] = 'NONE'
        
    return new_custom_dimension_item


def extract_user_defined_dimension(
    review_text: str,
    mc_polarities: List[str],
    dimension_name: str,
    label_name: str,
    keywords: List[str],
    polarity_mapping: Dict[str, Union[float, int]]
) -> Union[Dict[str, Any], None]:
    """
    Main function for extracting custom dimension from a review text
    """
    custom_dimension_item = _extract_custom_dimension_from_keyword(
        review_text=review_text,
        dimension_name=dimension_name,
        label_name=label_name,
        keywords=keywords
    )
    if custom_dimension_item:  # dimension found
        # calculate polarity
        custom_dimension_item = _calculate_custom_dimension_score(
            custom_dimension_item=custom_dimension_item,
            mc_polarities=mc_polarities,
            polarity_mapping=polarity_mapping
        )
        return custom_dimension_item
        
    else:
        return None


def extract_user_defined_dimension_batch(
    row_batch: List[bigquery.table.Row],
    dimension_name: str,
    label_name: str,
    keywords: List[str],
    polarity_mapping: Dict[str, Union[str, int]],
    run_id: str,
    logger: logging.Logger
) -> List[Union[Dict[str, Any]]]:
    """
    Extract user defined dimension from a batch of bigquery Row
    """
    logger.info(
        f"[EXTRACT] Extracting user-defined dimensions: "
        f"{dimension_name}>{label_name} "
        f"with keywords: {keywords}"
    )
    results = []
    for row in row_batch:
        if row.trans_review:
            res = extract_user_defined_dimension(
                review_text=row.trans_review,
                mc_polarities=row.polarities,
                dimension_name=dimension_name,
                label_name=label_name,
                keywords=keywords,
                polarity_mapping=polarity_mapping
            )
            if res:
                new_row = dict(row)
                # pop some excess fields
                new_row.pop('polarities')
                # add some necessary fields
                # BQ load rows json does not accept datetime object
                new_row['created_at'] = str(datetime.utcnow())
                new_row['review_date'] = str(new_row['review_date'])
                new_row['run_id'] = run_id
                # update custom nlp results
                new_row.update(res)
                results.append(new_row)

    logger.info(f"[EXTRACT] Found {len(results)} reviews with user-defined dimensions")
    return results
                

def load_user_defined_dimension_to_table(
    table_id: str,
    user_defined_extractions: List[Dict[str, Any]],
    logger: logging.Logger
) -> bool:
    """
    Load user defined dimension extraction results to bigquery table
    """
    logger.info(
        f"[LOAD] Loading user-defined dimension extraction "
        f"results to table {table_id}..."
    )
    client = bigquery.Client()
    errors = client.insert_rows_json(
        json_rows=user_defined_extractions,
        table=table_id
    )
    if errors:
        logger.error(
            "[LOAD] Encountered errors while inserting rows: {}"
            .format(errors)
        )
        raise Exception("[LOAD] Load job fails!")
    
    logger.info(
        f"[LOAD] Load job successful: "
        f"table_name={table_id}, "
        f"num_items={len(user_defined_extractions)}, "
        f"first_item={user_defined_extractions[0]}"
    )
    
    return True
