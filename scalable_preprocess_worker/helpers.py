from collections import Counter
from curses.ascii import isdigit
import re
from typing import Optional, Tuple, Union

from dateparser import parse
import pycountry


def generate_unique_id(text):
    encoded_text = f"{base64.b64encode(text.encode('utf-8'))}"
    hashed_text = hashlib.sha512(encoded_text.encode("utf-8")).hexdigest()
    return hashed_text


def safe_dedup_g2_luminati_review(s, logger):
    """
    Safely dedup G2 reviews given by Luminati ONLY
    """
    try:
        s_array = s.split("Show MoreShow Less")
        # if Show MoreShow Less split token is not available, stop
        if len(s_array) in (0, 1):
            return s

        # check if all words in the first part is in the second part
        first_in_second = all(
            [True if t in s_array[1] else False for t in s_array[0].split(" ")]
        )
        # check if all words in the second part is in the first part
        second_in_first = all(
            [True if t in s_array[0] else False for t in s_array[1].split(" ")]
        )
        if first_in_second and second_in_first:
            # 2 parts are equivalent. Get first part
            return s_array[0]
        elif first_in_second:
            # all first in second => return second
            return s_array[1]
        elif second_in_first:
            # all second in first => return first
            return s_array[0]
        else:
            return s
    except:
        logger.exception("Cannot dedup review, returning the original one.")
        return s


def split_g2_review(text, logger):
    """
    Split G2 review text into technical types

    Params
    ------
    text: str: review text

    Return
    ------
    dict
    """
    try:
        if "Recommendations to others considering the product" in text:
            categories = ("pros", "cons", "recommendations", "problems_solved")
            match_results = re.findall(
                r"(?:What do you like best\?)(.*)"
                r"(?:What do you dislike\?)(.*)"
                r"(?:Recommendations to others considering the product:)(.*)"
                r"(?:What problems are you solving with the product\?\s*What benefits have you realized\?)(.*)",
                text,
            )
            if match_results:
                result_dict = dict(zip(categories, match_results[0]))
            else:
                result_dict = {"all": text}
        else:
            categories = ("pros", "cons", "problems_solved")
            match_results = re.findall(
                r"(?:What do you like best\?)(.*)"
                r"(?:What do you dislike\?)(.*)"
                r"(?:What problems are you solving with the product\?\s*What benefits have you realized\?)(.*)",
                text,
            )
            if match_results:
                result_dict = dict(zip(categories, match_results[0]))
            else:
                result_dict = {"all": text}

        return result_dict

    except:
        logger.exception("Error while splitting G2 technical review")
        return {"all": text}


def search_country_mouthshut(
    country_string: Optional[str],
) -> Union[Tuple[None, None], Tuple[str, str]]:
    """
    First remove special character from country string then do fuzzy search
    If first fuzzy search fails, split the string and search on each token
    Return the not-null one with the most occurences
    """
    NONE_RESULT = (None, None)

    def _fuzzy_search_country(inp: str) -> Union[Tuple[None, None], Tuple[str, str]]:
        """Search name and id, return (None, None) if search fails"""
        try:
            res = pycountry.countries.search_fuzzy(inp)
            return (res[0].name, res[0].alpha_2)
        except LookupError:
            return NONE_RESULT

    def _search_country_with_city(
        search_country_string: Optional[str],
    ) -> Union[Tuple[None, None], Tuple[str, str]]:

        # check on each token
        splitted = search_country_string.split()
        # next fuzzy step is expensive so check for early exit first
        if len(splitted) == 1:
            return NONE_RESULT

        counter = Counter([sp for sp in map(_fuzzy_search_country, splitted) if sp])
        if not counter:  # empty counter
            return NONE_RESULT

        sorted_counter = sorted(counter.items(), key=lambda item: item[1], reverse=True)
        country_name = sorted_counter[0][
            0
        ]  # already a tuple, and take the key (1st position)
        return country_name

    try:
        # exit in case of None, or empty string
        if not country_string:
            return NONE_RESULT

        processed_country_string = re.sub(r"[\,\.\-\_]", "", country_string).strip()
        if not processed_country_string:
            return NONE_RESULT
        res = pycountry.countries.search_fuzzy(processed_country_string)
        return res[0].name, res[0].alpha_2
    except LookupError:
        return _search_country_with_city(processed_country_string)


def parse_date_from_string(
    date_string: str, first_day: bool = True, date_only: bool = True
):
    if date_string is None:
        return None

    s = date_string.strip()
    if first_day:
        settings = {"PREFER_DAY_OF_MONTH": "first"}
    else:
        settings = {"PREFER_DAY_OF_MONTH": "last"}
    # parse
    dt = parse(s, settings=settings)
    if not dt:
        return dt
    # format
    if s.isdigit() and len(s) == 4:  # check if contains only year
        if first_day:
            dt = dt.replace(month=1, day=1)
        else:
            dt = dt.replace(month=12, day=31)

    if date_only:
        return dt.date().isoformat()
    else:
        return dt.isoformat()
