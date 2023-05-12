import subprocess
import re

import spacy

import config


# initiate spacy pipeline
def initialize_spacy_pipeline(model_name='en_core_web_md', 
                              download=True,
                              exclude_stopwords=config.EXCLUDE_STOPWORDS,
                              include_stopwords=config.INCLUDE_STOPWORDS,
                              logger=None):
    """
    Initialize a spacy pipeline from pretrained model
    """
    logger.info('[SOURCE] Initializing spaCy pipeline...')
    try:
        pipeline = spacy.load(model_name)
    
    except OSError as e:
        logger.warning(f'[SOURCE] Model {model_name} not available')
        if download:
            logger.info(f'[SOURCE] Downloading model {model_name}...')
            model_download_process = subprocess.run(['python', '-m', 'spacy', 'download', model_name])
            if model_download_process.returncode == 0:
                logger.info(f'[SOURCE] Model {model_name} downloaded successfully')
                pipeline = spacy.load(model_name)
            else:
                logger.error(f'[SOURCE] Unable to download model')
                return None
    except:
        logger.exception("[SOURCE] Some exception happens during loading")
        raise
    
    # exclude custom stopwords
    for w in exclude_stopwords:
        try:
            pipeline.Defaults.stop_words.remove(w)
        except:
            continue
    
    # include custom stopwords
    for w in include_stopwords:
        pipeline.Defaults.stop_words.add(w)
        
    logger.info('[SOURCE] SpaCy pipeline initialized successfully')
    return pipeline


def light_preprocess(text):
    """
    Lightly preprocess a given document. Includes, in order:
    - remove emojis
    - remove slashes and backslashes
    - add space to before opening parentheses and after closing parentheses (also convert to round bracket)
    - strip trailing and leading whitespaces
    - strip multiple whitespaces
    """
    # remove emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251" 
        "]+"
    )
    slash_backslash_pattern = re.compile(r'[\\\/]')
    open_parentheses_pattern = re.compile(r"(\S)[\(\[\{]")
    close_parentheses_pattern = re.compile(r"(\S)[\)\]\}]")
    special_character_pattern = re.compile(r"[\_\@\#\$\^\*\+\=]")

    # preprocess each document
    preprocessed_text = text[:]
    preprocessed_text = emoji_pattern.sub(' ', preprocessed_text) # remove emojis
    preprocessed_text = slash_backslash_pattern.sub(' ', preprocessed_text)
    preprocessed_text = open_parentheses_pattern.sub(r'\1 (', preprocessed_text)
    preprocessed_text = close_parentheses_pattern.sub(r'\1) ', preprocessed_text)
    preprocessed_text = special_character_pattern.sub(' ', preprocessed_text)
    preprocessed_text = preprocessed_text.strip() # strip leading and trailing whitespace
    preprocessed_text = ' '.join(preprocessed_text.split()) # remove mutiple spaces between words
    return preprocessed_text


# parser
def _convert_to_not(token, do_lemma=True):
    if token.dep_ == 'neg' and token.pos_ == 'PART':
        return token.norm_.strip()
    
    if do_lemma:
        return token.lemma_.strip()
    
    return token.text


def parse_noun_chunks(doc, 
                      do_lemma=True, 
                      do_lower=True, 
                      return_base_chunk=False):
    final = []
    base_final = []
    for tok in doc:
        if tok.is_punct or tok.is_stop:
            continue
        verb = []
        noun_chunk = []
        base_noun_chunk = []
        if tok.pos_ in ('NOUN', 'PROPN'):
            if tok.dep_ == 'compound':
                continue
                
            # ...to get the full compound nouns and/or associated adjectives and adverbs
            noun_chunk.append(tok.i)
            base_noun_chunk.append(tok.i)
            for left in tok.lefts:
                # extract compound nouns
                if left.dep_ == 'compound' and left.pos_ in ('NOUN', 'PROPN'):
                    noun_chunk.append(left.i)
                    base_noun_chunk.append(left.i)
                    for l in left.lefts:
                        # compound of compound
                        if l.dep_ == 'compound' and left.pos_ in ('NOUN', 'PROPN'):
                            noun_chunk.append(l.i)
                            base_noun_chunk.append(l.i)
                # extract associated adjectives 
                if left.dep_ == 'amod':
                    noun_chunk.append(left.i)
                    for l in left.lefts:
                        # adverbs of said adjective
                        if l.dep_ == 'advmod':
                            noun_chunk.append(l.i)
                
                # negative definite article
                if left.text == 'no':
                    noun_chunk.append(left.i)
                
            # ...to get the associated verb
            i = 0
            tmp_tok = tok
            while i < 2: # at most 2 heads away
                _head = tmp_tok.head
                if _head.pos_ == 'VERB' and tok.dep_ in ('dobj', 'ROOT'):
                    verb.append(_head.i)
                    for left in _head.lefts:
                        if left.dep_ == 'neg':
                            verb.append(left.i)
                            break
                    break # stop if has found the closest verb
                tmp_tok = _head
                i += 1
                
            # ...get text, lemmatize if applicable
            text = ' '.join(
                [_convert_to_not(doc[i], do_lemma=do_lemma) 
                    for i in list(sorted(verb + noun_chunk))
                    if not doc[i].is_stop or doc[i].text in config.EXCLUDE_STOPWORDS]
            )
            base_text = ' '.join(
                [_convert_to_not(doc[i], do_lemma=do_lemma) 
                    for i in list(sorted(base_noun_chunk))
                    if not doc[i].is_stop or doc[i].text in config.EXCLUDE_STOPWORDS]
            )
            
            # ...lowercasing if applicable
            if do_lower:
                text = text.lower()
                base_text = base_text.lower()
            
            final.append(text)
            base_final.append(base_text)
    
    if return_base_chunk:
        return final, base_final
    else:
        return final


def parse_adjective_chunks(doc, 
                           do_lemma=True, 
                           do_lower=True, 
                           return_base_chunk=False):
    final = []
    base_final = []
    for tok in doc:
        if tok.is_punct or tok.is_stop:
            continue
            
        adj_chunk = []
        base_adj_chunk = []

        if tok.pos_ == 'ADJ' and tok.dep_ not in ('amod'):
            adj_chunk.append(tok.i)
            base_adj_chunk.append(tok.i)
            for left in tok.lefts:
                if left.pos_ in ('ADJ', 'ADV',) and left.dep_ in ('advmod',):
                    adj_chunk.append(left.i)
                if left.dep_ == 'neg':
                    adj_chunk.append(left.i)
                    
        if adj_chunk:
            adj_chunk = list(sorted(adj_chunk))
            # get text, lemmatize if applicable
            text = ' '.join([_convert_to_not(doc[i], do_lemma=do_lemma) 
                                for i in adj_chunk 
                                if not doc[i].is_stop or doc[i].text in config.EXCLUDE_STOPWORDS])
            base_text = ' '.join([_convert_to_not(doc[i], do_lemma=do_lemma) 
                                for i in base_adj_chunk 
                                if not doc[i].is_stop or doc[i].text in config.EXCLUDE_STOPWORDS])
            # lowercasing
            if do_lower:
                text = text.lower()
                base_text = base_text.lower()
                
            final.append(text)
            base_final.append(base_text)
    
    if return_base_chunk:
        return final, base_final
    else:
        return final
