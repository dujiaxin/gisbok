import os
import pandas as pd
import numpy as np
import re
import nltk

from nltk.stem.porter import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
# Function for sorting tf_idf in descending order
from scipy.sparse import coo_matrix

from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet
import nltk
from nltk.tokenize import word_tokenize, RegexpTokenizer
import os

import re
from nltk.corpus import stopwords
import json
import random
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score
from sklearn.multiclass import OneVsRestClassifier
from nltk.corpus import stopwords
from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from sklearn.metrics import classification_report
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def replace_marks(string, maxsplit=0):
    # replace special marks
    # string.replace('\\n','').replace('\\t','')
    markers = "*", "/", "+"
    regexPattern = '|'.join(map(re.escape, markers))
    return re.sub(regexPattern, ' ', string)


def read_website():
    with open('./data/questions/gis_topics.txt') as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    content = [x.strip() for x in content]
    return content


if __name__ == "__main__":
    # words we don't want to analyse
    stopwords_file = './stopwords.txt'
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    with open(stopwords_file, 'r', encoding='utf-8') as f:
        custom_stopwords = set(f.read().splitlines())
    all_stopwords = default_stopwords | custom_stopwords

    docs = read_website()
    stem = PorterStemmer()
    tfidf_vectorizer = TfidfVectorizer(stop_words=all_stopwords,
                            ngram_range=(1, 2),
                            token_pattern=u'(?ui)\\b\\w*[a-zA-Z]+\\w*\\b'
                            )
    embeded_docs = tfidf_vectorizer.fit_transform(docs)
    similarity_score_matrix = (embeded_docs * (embeded_docs.T)).toarray()
    index_very_similar = np.argwhere(similarity_score_matrix>0.5) #similarity_score_matrix is a symetic square matrix
    print(index_very_similar)