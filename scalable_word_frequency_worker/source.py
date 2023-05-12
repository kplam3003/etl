import sys
import os
from copy import deepcopy

import numpy as np
from scipy import sparse
from scipy.sparse import linalg


class Corpus:
    """
    Corpus object to save text, its keywords, and ids.
    """
    def __init__(self, corpus_with_keywords={}, metadata={}):
        """        
        corpus_with_keywords is dict of form:
        {
            'id': {
                'text': 'this is the text that may contain a list of keywords',
                'keywords': ['list', 'of', 'keywords'] # can be an empty list
            }
        }
        """
        # initilization
        self._corpus = deepcopy(corpus_with_keywords)
        self._metadata = {}
        if len(corpus_with_keywords) != 0:
            self._build_vocabulary()
                
    @property
    def vocab_size(self):
        return len(self._vocabs)
    
    @property
    def corpus_size(self):
        return len(self._corpus)
    
    @property
    def vocabs(self):
        return self._vocabs
    
    @property
    def corpus(self):
        return self._text_dict
    
    @property
    def metadata(self):
        return self._metadata
    
    def add_metadata(self, key, value):
        self._metadata.update({key: value})
        return True
    
    def add_documents(self, new_corpus_with_keywords, rebuild_vocab=True):
        # get new keywords
        self._corpus.update(new_corpus_with_keywords)
        if rebuild_vocab:
            self._build_vocabulary()

    def build_vocabulary(self):
        self._build_vocabulary()

    def _build_vocabulary(self):
        
        # get all keywords
        all_keywords = []
        for i, item in self._corpus.items():
            _text = item.get('text', '')
            _kwds = item.get('keywords', [])
            all_keywords.extend(_kwds)
        
        # create vocab and assign indices
        vocabs = list(set(all_keywords))
        idx = list(range(len(vocabs)))
        # save
        self._vocabs = np.array(vocabs)
        self._vocabs_ngram = np.array([len(v.split()) for v in self._vocabs])
        self._map = dict(zip(idx, vocabs))
        self._pam = dict(zip(vocabs, idx))
        
        # create a dict of reference
        self._kw_dict = {}
        self._text_dict = {}
        for i, item in self._corpus.items():
            _text = item.get('text', '')
            _kwds = item.get('keywords', [])
            _kwds_idx = map(lambda x: self._pam.get(x, -1), _kwds)
            self._kw_dict.update({
                i: list(_kwds_idx)
            })
            self._text_dict.update({
                i: _text
            })


def compute_tf_matrix(idx_array_list, vocab_size):
    """
    Compute the Term Frequency of given index array list and vocab size.
    
    Param:
    ----
    idx_array_list: list of list of indices of keywords, from Corpus object. Each component list
        corresponds to one document/text
    vocab_size: total number of unique keywords, from Corpus object
    
    Return:
    a scipy's Compressed Sparse Row matrix of size (len(idx_array_list) x vocab_size)
    """
    non_empty_array_list = [np.array(li) for li in idx_array_list if li]
    nrow = len(non_empty_array_list)
    tf_sparse_matrix = sparse.lil_matrix((nrow, vocab_size), dtype='int')
    for i in np.arange(nrow):
        _unique, _counts = np.unique(non_empty_array_list[i], return_counts=True)
        tf_sparse_matrix[i, _unique] = _counts
        
    return tf_sparse_matrix.tocsr()


def compute_tfidf(tf_matrix, normalize=True): 
    """
    Compute the N-Gram Scaled (Nomalized) TF-IDF of given initial Term Frequency (sparse) matrix.
    
    Param:
    ----
    tf_matrix: scipy CSR matrix: term frequency, rows are documents, columns are keywords
    normalize: bool: if True, do L2 Normalization on the term frequency of each document (row). 
        If False, use pure term frequency.
    
    Return:
    a tuple of size 2: (
        document frequency vector, 
        N-Gram Scaled (Nomalized) TF-IDF vector
    )
    """
    # calculate term frequency
    if normalize:
        weighted_tf_matrix = tf_matrix.multiply(1 / linalg.norm(tf_matrix, axis=1).reshape(-1, 1))
        corpus_tf_matrix = np.sum(weighted_tf_matrix, axis=0)
    else:
        corpus_tf_matrix = np.sum(tf_matrix, axis=0)
        
    # calculate inversed document frequency
    tf_matrix_copy = tf_matrix.copy()
    tf_matrix_copy[tf_matrix_copy.nonzero()] = 1
    df_matrix = np.sum(tf_matrix_copy, axis=0)
    idf_matrix = np.log(tf_matrix_copy.shape[0] / df_matrix)
    tfidf_matrix = np.multiply(corpus_tf_matrix, idf_matrix)
    
    return np.ravel(df_matrix), np.ravel(tfidf_matrix)
    

    