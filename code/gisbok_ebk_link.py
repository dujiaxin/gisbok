import pandas as pd
import json
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np
import spacy
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize


def get_stopwords():
    stopwords_file = './stopwords.txt'
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    with open(stopwords_file, 'r', encoding='utf-8') as f:
        custom_stopwords = set(f.read().splitlines())
    all_stopwords = default_stopwords | custom_stopwords
    return all_stopwords


def stem_lines(line, ps=PorterStemmer()):
    tokens = word_tokenize(line)
    tokens = [word for word in tokens if len(word) > 1]
    stemmed_tokens = [ps.stem(i) for i in tokens]
    return ' '.join(stemmed_tokens)


if __name__ == "__main__":
    gisbok = pd.read_csv('./gisbok_knowledgeArea_result.csv')
    ebk = pd.read_csv('./EBK.csv')

    topics_ebk = ebk[["Topic"]]
    topics_gisbok = gisbok[["topic"]]
    # gisbok_json = json.load(gisbok.to_json(orient='records'))
    gisbok_docs = gisbok.to_csv(index=False).split('\r\n')
    ebk_docs = topics_ebk.to_csv(index=False).split('\r\n')
    ps = PorterStemmer()

    stems_gisbok = [stem_lines(i, ps) for i in gisbok_docs]
    stems_ebk = [stem_lines(i, ps) for i in ebk_docs]

    tfidf_vectorizer = TfidfVectorizer(stop_words=get_stopwords(),
                                       token_pattern=u'(?ui)\\b\\w*[a-zA-Z]+\\w*\\b'
                                       )

    tfidf_vectorizer.fit(stems_gisbok + stems_ebk)
    embedded_gisbok = tfidf_vectorizer.transform(stems_gisbok)
    embedded_ebk = tfidf_vectorizer.transform(stems_ebk)

    similarity_score_matrix = (embedded_gisbok * (embedded_ebk.T)).toarray()
    index_very_similar = np.argwhere(
        similarity_score_matrix > 0.1)  # similarity_score_matrix is a symetic square matrix
    print(len(index_very_similar))
    pairs = set()
    for i in index_very_similar:
        pairs.add(gisbok_docs[i[0]] + " ; " +ebk_docs[i[1]])
    print("\n".join(pairs))


    # nlp = spacy.load("en_core_web_lg")
    # gisbok_nlp = [nlp(i) for i in gisbok_docs]
    # ebk_nlp = [nlp(i) for i in ebk_docs]
    # for i, gis in enumerate(gisbok_nlp):
    #     for j, ebk in enumerate(ebk_nlp):
    #         if gis.similarity(ebk) >0.9:
    #             print(i,j)