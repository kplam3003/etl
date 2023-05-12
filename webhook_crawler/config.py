import os

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-api-translation')
LOGGER = "application"

GCP_PROJECT = os.environ.get('GCP_PROJECT')
GCP_STORAGE_BUCKET=os.environ.get('GCP_STORAGE_BUCKET')

LUMINATI_DCA_URL = os.environ.get('LUMINATI_DCA_URL', 'https://api.luminati.io/dca/trigger')
LUMINATI_DCA_ID = os.environ.get('LUMINATI_DCA_ID')
LUMINATI_DCA_ID_G2_REVIEWS = os.environ.get('LUMINATI_DCA_ID_G2_REVIEWS')
LUMINATI_DCA_ID_LINKEDIN_COMPANY_PROFILE = os.environ.get('LUMINATI_DCA_ID_LINKEDIN_COMPANY_PROFILE')
LUMINATI_DCA_TOKEN = os.environ.get('LUMINATI_DCA_TOKEN')

ROWS_PER_STEP = 100

SECRET = os.environ.get('SECRET', "TIDdITanFCHw5CD97pTIDdITanFCHw5CD97pTIDdITanFCHw5CD97p")
