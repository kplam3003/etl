import os

TASKS_DIR = os.environ.get("TASKS_DIR")
DATABASE_URI = os.environ.get("DATABASE_URI")
CRAWLER_IDS = os.environ.get("CRAWLER_IDS", "").split(",")
BATCH_NAME = os.environ.get("BATCH_NAME")
BATCH_ID = int(os.environ.get("BATCH_ID", "0"))

# Crawler config
STEP_PAGING = 10
ROWS_PER_STEP = int(os.environ.get("ROWS_PER_STEP", "200"))

CAPTERRA_PAGE_SIZE = 25
CAPTERRA_MAX_ITEMS = 10000

APPLESTORE_PAGE_SIZE = 10
APPLESTORE_LANGS = ["us", "gb", "vn", "au", "ca", "fr"]

TRUSTPILOT_PAGE_SIZE = 25

G2_PAGE_SIZE = 25

LUMI_AUTH_TOKEN = "zymetHXUphazpPnQbEJqrXy5a4MFqFQs"  # TODO: only temp, will move to env var and deployment script

# Glassdoor datasource
GLASSDOOR_REVIEW_PAGE_SIZE = 10
GLASSDOOR_JOB_PAGE_SIZE = 40

# Indeed datasource
INDEED_REVIEW_PAGE_SIZE = 20
INDEED_JOB_PAGE_SIZE = 100

# LinkedIn datasource
LINKEDIN_JOB_PAGE_SIZE = 25

# GARTNER config
GARTNER_REVIEW_PAGE_SIZE = 15

# Moutshut config
MOUTHSHUT_REVIEW_PAGE_SIZE = 20

# Ambitionbox config
AMBITIONBOX_PAGE_SIZE = 10

WEB_WRAPPER_URL = os.environ.get("WEB_WRAPPER_URL", "http://34.121.81.95")

GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_PUBSUB_SUBSCRIPTION_DISPATCHER = os.environ.get(
    "GCP_PUBSUB_SUBSCRIPTION_DISPATCHER"
)
GCP_PUBSUB_TOPIC_DISPATCH = os.environ.get("GCP_PUBSUB_TOPIC_DISPATCH")
GCP_PUBSUB_TOPIC_CRAWL = os.environ.get("GCP_PUBSUB_TOPIC_CRAWL")
GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR = os.environ.get(
    "GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR"
)
GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR = os.environ.get(
    "GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR"
)

# Logging
LOGGER_NAME = os.environ.get("LOGGER_NAME", "dev-etl-dispatcher")
LOGGER = "application"

SLEEP = int(os.environ.get("SLEEP", "30"))

# Datashake
DATASHAKE_API = os.environ.get("DATASHAKE_API", "https://app.datashake.com/api/v2")
DATASHAKE_TOKEN = os.environ.get("DATASHAKE_TOKEN")

LUMINATI_HTTP_PROXY = os.environ.get("LUMINATI_HTTP_PROXY")
LUMINATI_WEBUNBLOCKER_HTTP_PROXY = os.environ.get("LUMINATI_WEBUNBLOCKER_HTTP_PROXY")
WEBHOOK_API = os.environ.get("WEBHOOK_API")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN")
WEBHOOK_PAGE_SIZE = 100

# Apple Countries
APPLE_COUNTRIES = [
    "ae",
    "ag",
    "ai",
    "al",
    "am",
    "ao",
    "ar",
    "at",
    "au",
    "az",
    "bb",
    "be",
    "bf",
    "bg",
    "bh",
    "bj",
    "bm",
    "bn",
    "bo",
    "br",
    "bs",
    "bt",
    "bw",
    "by",
    "bz",
    "ca",
    "cg",
    "ch",
    "cl",
    "cn",
    "co",
    "cr",
    "cv",
    "cy",
    "cz",
    "de",
    "dk",
    "dm",
    "do",
    "dz",
    "ec",
    "ee",
    "eg",
    "es",
    "fi",
    "fj",
    "fm",
    "fr",
    "gb",
    "gd",
    "gh",
    "gm",
    "gr",
    "gt",
    "gw",
    "gy",
    "hk",
    "hn",
    "hr",
    "hu",
    "id",
    "ie",
    "il",
    "in",
    "is",
    "it",
    "jm",
    "jo",
    "jp",
    "ke",
    "kg",
    "kh",
    "kn",
    "kr",
    "kw",
    "ky",
    "kz",
    "la",
    "lb",
    "lc",
    "lk",
    "lr",
    "lt",
    "lu",
    "lv",
    "md",
    "mg",
    "mk",
    "ml",
    "mn",
    "mo",
    "mr",
    "ms",
    "mt",
    "mu",
    "mw",
    "mx",
    "my",
    "mz",
    "na",
    "ne",
    "ng",
    "ni",
    "nl",
    "np",
    "no",
    "nz",
    "om",
    "pa",
    "pe",
    "pg",
    "ph",
    "pk",
    "pl",
    "pt",
    "pw",
    "py",
    "qa",
    "ro",
    "ru",
    "sa",
    "sb",
    "sc",
    "se",
    "sg",
    "si",
    "sk",
    "sl",
    "sn",
    "sr",
    "st",
    "sv",
    "sz",
    "tc",
    "td",
    "th",
    "tj",
    "tm",
    "tn",
    "tr",
    "tt",
    "tw",
    "tz",
    "ua",
    "ug",
    "us",
    "uy",
    "uz",
    "vc",
    "ve",
    "vg",
    "vn",
    "ye",
    "za",
    "zw",
]

# Indeed countries
INDEED_COUNTRIES = [
    "www",
    "ar",
    "au",
    "at",
    "bh",
    "be",
    "br",
    "ca",
    "cl",
    "cn",
    "co",
    "cr",
    "cz",
    "dk",
    "ec",
    "eg",
    "fi",
    "fr",
    "de",
    "gr",
    "hk",
    "hu",
    "in",
    "id",
    "ie",
    "il",
    "it",
    "jp",
    "kw",
    "lu",
    "malaysia",
    "mx",
    "ma",
    "nl",
    "nz",
    "ng",
    "no",
    "om",
    "pk",
    "pa",
    "pe",
    "ph",
    "pl",
    "pt",
    "qa",
    "ro",
    "ru",
    "sa",
    "sg",
    "za",
    "kr",
    "es",
    "se",
    "ch",
    "tw",
    "th",
    "tr",
    "ua",
    "ae",
    "uk",
    "uy",
    "ve",
    "vn",
]

# Glassdoor countries
GLASSDOOR_COUNTRIES_ID2NAME_MAP = {
    "1": "United States",
    "2": "United Kingdom",
    "3": "Canada",
    "6": "United Arab Emirates",
    "15": "Argentina",
    "16": "Australia",
    "18": "Austria",
    "25": "Belgium",
    "36": "Brazil",
    "49": "Chile",
    "54": "Colombia",
    "63": "Denmark",
    "69": "Egypt",
    "70": "Ireland",
    "77": "Czech Republic",
    "86": "France",
    "89": "Gabon",
    "91": "Ghana",
    "96": "Germany",
    "106": "Hong Kong",
    "113": "Indonesia",
    "115": "India",
    "119": "Israel",
    "120": "Italy",
    "130": "Kenya",
    "135": "South Korea",
    "162": "Morocco",
    "169": "Mexico",
    "170": "Malaysia",
    "171": "Mozambique",
    "177": "Nigeria",
    "178": "Netherlands",
    "186": "New Zealand",
    "192": "Pakistan",
    "193": "Poland",
    "195": "Portugal",
    "199": "Qatar",
    "203": "Romania",
    "204": "Philippines",
    "207": "Saudi Arabia",
    "211": "South Africa",
    "217": "Singapore",
    "219": "Spain",
    "223": "Sweden",
    "226": "Switzerland",
    "238": "Turkey",
    "251": "Vietnam",
    "254": "Namibia",
    "262": "Zambia",
}
# Google Play countries
GOOGLEPLAY_COUNTRIES = [
    "us",
    "al",
    "dz",
    "ao",
    "ag",
    "ar",
    "am",
    "aw",
    "au",
    "at",
    "az",
    "bs",
    "bh",
    "bd",
    "by",
    "be",
    "bz",
    "bj",
    "bo",
    "ba",
    "bw",
    "br",
    "bg",
    "bf",
    "kh",
    "cm",
    "ca",
    "cl",
    "co",
    "cr",
    "hr",
    "cy",
    "cz",
    "dk",
    "do",
    "ec",
    "eg",
    "sv",
    "ee",
    "fj",
    "fi",
    "fr",
    "ga",
    "ge",
    "de",
    "gh",
    "gi",
    "gr",
    "gt",
    "gw",
    "ht",
    "hn",
    "hk",
    "hu",
    "is",
    "in",
    "id",
    "iq",
    "ie",
    "il",
    "it",
    "jm",
    "jp",
    "jo",
    "kz",
    "ke",
    "kw",
    "kg",
    "lv",
    "lb",
    "li",
    "lt",
    "lu",
    "mk",
    "my",
    "ml",
    "mt",
    "mu",
    "mx",
    "md",
    "mc",
    "ma",
    "mz",
    "mm",
    "na",
    "np",
    "nl",
    "nz",
    "ni",
    "ng",
    "ng",
    "no",
    "om",
    "pk",
    "pa",
    "pg",
    "py",
    "pe",
    "ph",
    "pl",
    "pt",
    "qa",
    "ro",
    "ru",
    "rw",
    "sm",
    "sa",
    "sn",
    "rs",
    "sg",
    "sk",
    "si",
    "za",
    "kr",
    "es",
    "lk",
    "se",
    "ch",
    "tw",
    "tj",
    "tz",
    "th",
    "tg",
    "tt",
    "tn",
    "tr",
    "tm",
    "ug",
    "ua",
    "ae",
    "gb",
    "us",
    "uy",
    "uz",
    "ve",
    "vn",
    "ye",
    "zm",
    "zw",
]

GOOGLEPLAY_SUPPORTED_LANGS = [
    ("Afrikaans", "af"),
    ("Amharic", "am"),
    ("Bulgarian", "bg"),
    ("Catalan", "ca"),
    ("Chinese", "zh"),
    ("Croatian", "hr"),
    ("Czech", "cs"),
    ("Danish", "da"),
    ("Dutch", "nl"),
    ("English", "en"),
    ("Estonian", "et"),
    ("Filipino", "fil"),
    ("Finnish", "fi"),
    ("French", "fr"),
    ("German", "de"),
    ("Greek", "el"),
    ("Hebrew", "he"),
    ("Hindi", "hi"),
    ("Hungarian", "hu"),
    ("Icelandic", "is"),
    ("Indonesian", "id"),
    ("Italian", "it"),
    ("Japanese", "ja"),
    ("Korean", "ko"),
    ("Latvian", "lv"),
    ("Lithuanian", "lt"),
    ("Malay", "ms"),
    ("Norwegian", "no"),
    ("Polish", "pl"),
    ("Portuguese", "pt"),
    ("Romanian", "ro"),
    ("Russian", "ru"),
    ("Serbian", "sr"),
    ("Slovak", "sk"),
    ("Slovenian", "sl"),
    ("Spanish", "es"),
    ("Swahili", "sw"),
    ("Swedish", "sv"),
    ("Thai", "th"),
    ("Turkish", "tr"),
    ("Ukrainian", "uk"),
    ("Vietnamese", "vi"),
    ("Zulu", "zu"),
    ("Hongkong", "zh-HK"),
    ("Taiwan", "zh-TW"),
]
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "8zlXlxsFGrW9dQ")
REDDIT_CLIENT_SECRET = os.environ.get(
    "REDDIT_CLIENT_SECRET", "NX207Ak2e2hCfdzrh9qm647sWy3EBQ"
)
REDDIT_PAGE_SIZE = 200

# Coresignal
CORESIGNAL_JWT = os.environ.get("CORESIGNAL_JWT")
CORESIGNAL_EMPLOYEE_PAGE_SIZE = 20
CORESIGNAL_SEARCH_RESULT_MAX_SIZE = 1000

# MONGODB
MONGODB_DATABASE_URI = os.environ.get(
    "MONGODB_DATABASE_URI", "mongodb://172.16.11.24:27018"
)
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dev_dp_leonardo_db")
MONGODB_DATABASE_ROOT_CA = os.environ.get(
    "MONGODB_DATABASE_ROOT_CA", "/app/certs/root-ca.pem"
)
MONGODB_DATABASE_KEY_FILE = os.environ.get(
    "MONGODB_DATABASE_KEY_FILE", "/app/certs/dpworker.pem"
)

MONGODB_VOC_REVIEW_COLLECTION = "voc"
MONGODB_VOE_REVIEW_COLLECTION = "voe"
MONGODB_VOE_OVERVIEW_COLLECTION = "voe_overview"
MONGODB_VOE_JOB_COLLECTION = "voe_job"
MONGODB_CORESIGNAL_STATS_COLLECTION = "coresignal_stats"
MONGODB_CORESIGNAL_EMPLOYEES_COLLECTION = "coresignal_employees"
MONGODB_CORESIGNAL_CD_MAPPING_COLLECTION = "coresignal_company_datasource"

# BigQuery tables
GCP_BQ_TABLE_VOC_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_VOC_CRAWL_STATISTICS", "leo-etlplatform.dwh.voc_crawl_statistics"
)
GCP_BQ_TABLE_VOE_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_VOE_CRAWL_STATISTICS", "leo-etlplatform.dwh.voe_crawl_statistics"
)
GCP_BQ_TABLE_HRA_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_HRA_CRAWL_STATISTICS", "leo-etlplatform.dwh.hra_crawl_statistics"
)
