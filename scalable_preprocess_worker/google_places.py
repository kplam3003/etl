from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import logging

import googlemaps
import pymongo

import config


GOOGLE_FIND_PLACES_URL = (
    "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
)
RESPONSE_FIELDS = [
    "business_status",
    "formatted_address",
    "geometry",
    "icon",
    "icon_mask_base_uri",
    "icon_background_color",
    "name",
    "photo",
    "place_id",
    "plus_code",
]
RESPONSE_ERROR_STATUSES = [
    "INVALID_REQUEST",
    "OVER_QUERY_LIMIT",
    "REQUEST_DENIED",
    "UNKNOWN_ERROR",
]
# Init mongodb
client = pymongo.MongoClient(
    config.MONGODB_DATABASE_URI,
    authMechanism="MONGODB-X509",
    tls=True,
    tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
    tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
    tlsAllowInvalidHostnames=True,
)
db = client.get_database(config.MONGODB_DATABASE_NAME)
gmaps = googlemaps.Client(key=config.GOOGLE_MAPS_API_KEY)


def search_places_from_api(
    search_location: str, logger: logging.Logger, ids_only: bool = True
) -> Optional[List[Dict]]:
    """Search for a given location using Google Maps Places API

    Args:
        search_location (str): query string for location
        logger (logging.Logger): logger
        ids_only (bool, optional): whether to return found ids only. Id-only
            queries are free. Defaults to True.

    Returns:
        Optional[List[Dict]]: a list of dicts that are candidates for search location
        Returns None if request returns an error.
    """
    find_place_response: Dict = gmaps.find_place(
        input=search_location,
        fields=None if ids_only else RESPONSE_FIELDS,
        input_type="textquery",
        language="en",
    )
    status: Optional[str] = find_place_response.get("status")

    if status in RESPONSE_ERROR_STATUSES:
        error_message: Optional[str] = find_place_response.get("error_message")
        logger.error(
            f"Error happens while searching for location: {search_location} - "
            f"{status}: {error_message}"
        )
        return None

    candidates: List[Dict] = find_place_response.get("candidates", [])
    return candidates


def check_places_cache(place_id: str) -> Optional[Dict]:
    collection = db.get_collection(config.MONGODB_GOOGLE_MAPS_PLACES_CACHE)
    item = collection.find_one(
        filter={"place_id": place_id},
        projection={"address_components": 1},
    )
    return item


def write_place_to_cache(
    original_search_location: str, items: List[Dict[str, Any]], logger: logging.Logger
) -> bool:
    inserted_datetime = datetime.utcnow().isoformat()
    for item in items:
        # add info to item
        item["original_search_location"] = original_search_location
        item["created_at"] = inserted_datetime

    # perform insert
    collection = db.get_collection(config.MONGODB_GOOGLE_MAPS_PLACES_CACHE)
    try:
        insert_result = collection.insert_many(items, ordered=False)
        return insert_result.acknowledged

    except pymongo.errors.BulkWriteError as err:
        logger.warning(
            f"Error happens while writing to Google Map Place cache. Exception: {err}"
        )
        return False


def extract_address_components(
    address_components: Optional[List[dict]],
) -> Tuple[str, str]:
    """
    Extract `short_name` of types `country` and `administrative_area_level_1` from `address_components`.
    """
    country = None
    admin1 = None

    components = [item for item in address_components if "country" in item["types"]]
    if components:
        country = components[0]["long_name"]

    components = [
        item
        for item in address_components
        if "administrative_area_level_1" in item["types"]
    ]
    if components:
        admin1 = components[0]["long_name"]

    return (country, admin1)


def parse_location(search_location: str, logger: logging.Logger):
    # return dict
    result = {
        "location_from_cache": False,
        "location_not_found": True,
        "google_place_id": None,
        "google_country": None,
        "google_admin1": None,
    }

    candidates = search_places_from_api(
        search_location=search_location, logger=logger, ids_only=True
    )
    # give back the original location
    if not candidates:
        return result

    # check from db cache
    candidate = candidates[0]
    candidate_place_id: str = candidate["place_id"]
    item = check_places_cache(candidate_place_id)
    if item:
        # cache hit, return item
        result["location_from_cache"] = True
        result["location_not_found"] = False
        result["google_place_id"] = candidate_place_id
        result["google_country"], result["google_admin1"] = extract_address_components(
            address_components=item["address_components"]
        )

    else:
        # cache miss, call Find Places API
        logger.warning(
            f"Cache missed for candidate_place_id: {candidate_place_id}. "
            "Will use Geocoding API to get info and update cache."
        )
        geocode_results = gmaps.geocode(place_id=candidate_place_id)
        write_place_to_cache(
            original_search_location=search_location,
            items=geocode_results,
            logger=logger,
        )
        result["location_not_found"] = False
        result["google_place_id"] = candidate_place_id
        result["google_country"], result["google_admin1"] = extract_address_components(
            address_components=geocode_results[0]["address_components"]
        )

    return result
