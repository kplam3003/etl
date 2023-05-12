# stdlibs
import json
import os
import logging
import sys
import base64
import hashlib

sys.path.append('../')

# 3rd party
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from typing import Optional, List
from pymemcache.client.base import Client
from sentence_transformers import SentenceTransformer

# own
import config
from core import logger
from job_classifier import BasicProcessor, JobClassifierInferenceSession


# init logger
logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER) # cloud logger

# local logger for debugging
# logger = logging.getLogger('scalable-prediction-service')
# logger.setLevel(logging.INFO)
# ch = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)

# init ML models
# job classifier
job_title_processor = BasicProcessor()
job_classifier = JobClassifierInferenceSession(project_id=config.GCP_PROJECT_ID, 
                                               bucket=config.GCP_STORAGE_BUCKET, 
                                               model_directory=config.JOB_CLASSIFIER_MODEL_DIRECTORY, 
                                               model_file=config.JOB_CLASSIFIER_MODEL_FILE, 
                                               label_mapping_file=config.JOB_CLASSIFIER_LABEL_MAPPING, 
                                               logger=logger)

# init memcache
cache_client = Client(config.MEMCACHED_SERVER)

# init app
app = FastAPI(title='leo-etl-prediction-service-app')
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

@app.get('/')
def hello():
    return "Welcome to Leonardo Data Platform's Prediction Service! Head to /docs to try this API!"


# endpoint for job-classifier
@app.post('/job-classifier/predict')
async def predict_job_function(response: Response, job_title: str, conf_thresh: Optional[float] = None):
    """
    Main entry point for extracting keyword
    """
    # preprocess
    processed_job_title = job_title_processor.process(job_title)
    
    # # check cache
    # cache_key = hashlib.sha512(processed_job_title.encode('utf-8'))
    # cache_key = "job_classifier" + base64.b64encode(cache_key.digest()).decode()
    
    # fetch from cache
    cache_hit = 'false'
    result = None
    # fetch from cache
    # try:
    #     result = None
    #     result = cache_client.get(cache_key) # get data from cache
    #     cache_hit = 'true'
    # except:
    #     logger.exception('Failed to fetch from cache!')
    
    if result is None:
        # cache miss
        prediction, confidence = job_classifier.predict(processed_job_title)
    
        if isinstance(conf_thresh, float):
            if confidence < conf_thresh:
                prediction = "Undefined"
        
        result = {"prediction": prediction, "confidence": confidence}
        
        # try:
        #     cache_client.set(cache_key, result)
        # except:
        #     logger.exception('Failed to update cache!')
    
    response.headers["cache_hit"] = cache_hit

    return result


@app.post("/encode-sentence")
async def encode_sentence(response: Response, sentences: List[str]):
    encoded_sentences = model.encode(sentences)
    return encoded_sentences.tolist()
