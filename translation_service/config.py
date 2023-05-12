import os

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-api-translation')
LOGGER = "application"

LUMINATI_HTTP_PROXY = os.environ.get('LUMINATI_HTTP_PROXY')

CACHE_DIR = os.environ.get('CACHE_DIR', '/tmp')
MEMCACHED_SERVER = os.environ.get('MEMCACHED_SERVER', 'localhost')
CACHE_ENABLE = int(os.environ.get('CACHE_ENABLE', '0'))

SECRET = os.environ.get('SECRET', "TIDdITanFCHw5CD97pTIDdITanFCHw5CD97pTIDdITanFCHw5CD97p")
