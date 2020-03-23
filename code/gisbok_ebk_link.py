import pandas as pd
import json
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np
import spacy


def get_stopwords():
    stopwords_file = './stopwords.txt'
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    with open(stopwords_file, 'r', encoding='utf-8') as f:
        custom_stopwords = set(f.read().splitlines())
    all_stopwords = default_stopwords | custom_stopwords
    return all_stopwords


if __name__ == "__main__":
    gisbok = pd.read_csv('./gisbok_knowledgeArea_result.csv')
    ebk = pd.read_csv('./EBK.csv')

    topics_ebk = ebk[["Competency","Topic","Sub-topic"]]
    # gisbok_json = json.load(gisbok.to_json(orient='records'))
    gisbok_docs = gisbok.to_csv().split('\r\n')
    ebk_docs = topics_ebk.to_csv().split('\r\n')


    # tfidf_vectorizer = TfidfVectorizer(stop_words=get_stopwords())

    nlp = spacy.load("en_core_web_lg")
    gisbok_nlp = [nlp(i) for i in gisbok_docs]
    ebk_nlp = [nlp(i) for i in ebk_docs]
    for i, gis in enumerate(gisbok_nlp):
        for j, ebk in enumerate(ebk_nlp):
            if gis.similarity(ebk) >0.9:
                print(i,j)


    # embeded_docs = tfidf_vectorizer.fit_transform(all_docs)
    # similarity_score_matrix = (embeded_docs * (embeded_docs.T)).toarray()
    # index_very_similar = np.argwhere(
    #     similarity_score_matrix > 0.5)  # similarity_score_matrix is a symetic square matrix
    # print(index_very_similar)
    # for i in index_very_similar:
    #     print(all_docs[i[0]] + " ; " +all_docs[i[1]])