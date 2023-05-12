# builtin
import sys
import re
import time
import json
import html
import logging
import os
from hashlib import md5
import base64

# 3rd party
from tqdm import tqdm
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import onnxruntime as rt

from google.cloud import storage


# own
sys.path.append('../')
import config


nltk.download('wordnet')
nltk.download('punkt')
nltk.download('stopwords')

# initiate nltk stopword set first
nltk_stopwords = stopwords.words('english')
_ = nltk_stopwords[0]
nltk_stopwords = set(nltk_stopwords)


def remove_html_tags(s):
    """Removce HTML tags from text"""
    # unescape html and remove tags
    patt = r"<.*?>"
    new_s = re.sub(patt, ' ', html.unescape(str(s)))
    
    # try to fix decoding errors
    try:
        bytes_string = bytes(new_s, encoding="raw_unicode_escape")
        new_s = bytes_string.decode("utf-8", "strict")
    except:
        pass
    
    return new_s


class BasicProcessor:
    """
    Just a basic processor class to avoid loading nltk everytime, or declaring them beforehand
    """
    def __init__(self, stem=False, stopword_set=None):
        self.tokenizer = RegexpTokenizer(r'[A-Za-z]+')
        self.lemmatizer = WordNetLemmatizer()
        self.stemmer = PorterStemmer()
        self.stem = stem
        
        if stopword_set:
            self.stopword_set = set(stopword_set)
        else:
            self.stopword_set = nltk_stopwords
        
    def process(self, text):
        """
        Lemmatize and remove stopwords
        """
        if self.stem:
            f = self.stemmer.stem
        else:
            f = self.lemmatizer.lemmatize

        tokens = [f(w).lower() 
                  for w in self.tokenizer.tokenize(text) 
                  if w.lower() not in self.stopword_set and len(w) > 2]
        
        return ' '.join(tokens)
    

class JobClassifierInferenceSession:
    def __init__(self, project_id, bucket, model_directory,
                 model_file, label_mapping_file, 
                 logger):
        # save settings
        self.project_id = project_id
        self.bucket = bucket
        self.model_directory = model_directory
        self.model_name = model_file
        self.label_mapping_name = label_mapping_file
        self.logger = logger
        # get paths
        self.model_path = os.path.join(model_directory, model_file)
        self.label_mapping_path = os.path.join(model_directory, label_mapping_file)
        
        # initial state
        self.session = None
        self.is_active = False
        self.input_name = None
        self.output_names = None
        self.label_dict = None
                
        # load model
        self._load_model()
        self._load_labels()
    
    def _get_md5_hash(self, local_file_path):
        """
        Compute the base64 md5 hash of a local file
        """
        with open(local_file_path, 'rb') as f:
            hasher = md5(f.read())
        base64_md5_digest = base64.b64encode(hasher.digest()).decode()
    
        return base64_md5_digest
    
    def _download_from_gcs(self, file='model'):
        if file == 'model':
            name = 'model binary'
            file_path = self.model_path
        elif file == 'label':
            name = 'label mapping'
            file_path = self.label_mapping_path
        else:
            name = 'model binary'
            file_path = self.model_path
            
        # Initialise a client
        storage_client = storage.Client(self.project_id)
        # Create a bucket object for our bucket
        bucket = storage_client.get_bucket(self.bucket)
        # Create a blob object from the filepath
        blob = bucket.blob(file_path)
        blob.reload()
        # check model binary integrity
        if os.path.exists(file_path):
            self.logger.info(f"Local {name} exists.")
            gcs_md5 = blob.md5_hash
            local_md5 = self._get_md5_hash(file_path)

            if local_md5 == gcs_md5:
                self.logger.info(f"Local {name} is identical to that in GCS.")
                # terminate
                return True
            else:
                self.logger.warning(f"Local {name} different from that in GCS! Removing...")
                os.remove(file_path)
        else:
            self.logger.warning(f"Local {name} does not exists!")

        # Download file from gcs
        self.logger.info(f"Downloading {name} from GCS...")
        os.makedirs(self.model_directory,  exist_ok=True) # create directory just to be sure
        with open(file_path, 'wb') as f:
            with tqdm.wrapattr(f, "write", total=blob.size) as file_obj:
                storage_client.download_blob_to_file(blob, file_obj)
                
        return True
        
    def _load_model(self):
        try:
            # download model from GCS
            self._download_from_gcs(file='model')
            self.logger.info("Loading ONNX model binary file into inference session...")
            _start_time = time.time()
            with open(self.model_path, 'rb') as f:
                self.session = rt.InferenceSession(f.read())
                self.logger.info(f"Model loaded successfully in {time.time() - _start_time:.2f} seconds!")
            
            self.is_active = True
            self.input_name = self.session.get_inputs()[0].name # only use preprocessed text as model input
            self.output_names = [o.name for o in self.session.get_outputs()]
            
        except:
            self.logger.exception(f"Cannot load ONNX model binary!")
            raise
            
    def _load_labels(self):
        """
        Label needs to be a json where 
        - keys are numerical mappings and 
        - values are associated text label
        """
        try:
            self._download_from_gcs(file='label')
            with open(self.label_mapping_path, 'r') as file:
                self.label_dict = json.load(file)
            self.logger.info("Label mapping loaded.")
        except:
            self.logger.exception(f"Cannot load label mappings!")
            raise
        
    def teminate_session(self):
        # delete session and garbage collect
        del self.session
        import gc
        gc.collect()
        
        # reset attributes
        self.session = None
        self.is_active = False
        self.input_name = None
        self.output_names = None
        self.label_dict = None
        
    def predict(self, single_input):
        try:
            # initial values
            pred_label = -1
            pred_label_text = ''
            pred_prob = 0.0

            pred = self.session.run(self.output_names, 
                                    {self.input_name: [[single_input]]})
            pred_label = pred[0][0]
            pred_label_text = self.label_dict[str(pred_label)]
            pred_prob = pred[1][0][pred_label]
            
        except:
            self.logger.exception(f"Prediction failed for input \"{single_input}\"")
            
        finally:
            return pred_label_text, pred_prob
            

if __name__ == "__main__":
    pass
    # # testing
    # # load config
    # from dotenv import load_dotenv
    # load_dotenv('config/.env')
    
    # # logger example
    # logger = logging.getLogger('test-classifier')
    # formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - %(filename)s : %(lineno)d - %(message)s")
    # stream_handler = logging.StreamHandler(sys.stdout)
    # stream_handler.setLevel(logging.INFO)
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)
    
    # # init session
    # session = JobClassifierInferenceSession(project_id=config.GCP_PROJECT_ID,
    #                                         bucket=config.GCP_STORAGE_BUCKET,
    #                                         model_directory=config.JOB_CLASSIFIER_MODEL_DIRECTORY,
    #                                         model_file=config.JOB_CLASSIFIER_MODEL_FILE,
    #                                         label_mapping_file=config.JOB_CLASSIFIER_LABEL_MAPPING,
    #                                         logger=logger)
    
    # processor = BasicProcessor()
    # example = 'Java Developer'
    
    # start = time.time()
    # print(session.predict(processor.process(example)))
    # print(time.time() - start) 
    
    # session.teminate_session()
    # del session
