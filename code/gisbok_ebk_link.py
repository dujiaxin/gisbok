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
    if len(line)<3:
        print(line)
        return 'null'
    tokens = word_tokenize(line)
    tokens = [word for word in tokens if len(word) > 1]
    stemmed_tokens = [ps.stem(i) for i in tokens]
    return ' '.join(stemmed_tokens)


def bow_topics(groupTopic, tfidf_vectorizer):
    embeddedTopic = []
    for name, group in groupTopic:
        # print(name)
        # print(group)
        topics = group['learning_objective'].to_list()
        stemmed = [stem_lines(i, ps) for i in topics]
        embedded_gisbok = tfidf_vectorizer.transform([" ".join(stemmed)])
        embeddedTopic.append({"topic":name, "embedding": embedded_gisbok.toarray()})
    return pd.DataFrame(embeddedTopic)


if __name__ == "__main__":
    gisbok = pd.read_csv('gisbok_lo.csv').fillna('null')#.fillna(method='ffill')
    ebk = pd.read_csv('./EBK_origin.csv').fillna(method='ffill')

    all_lo = gisbok["learning_objective"].to_list() + ebk["learning_objective"].to_list()

    topics_ebk = ebk["Topic"].fillna("null").to_list()
    topics_gisbok = gisbok["topic"].fillna("null").to_list()
    # gisbok_json = json.load(gisbok.to_json(orient='records'))
    ps = PorterStemmer()

    stems_gisbok = [stem_lines(i, ps) for i in topics_gisbok]
    stems_ebk = [stem_lines(i, ps) for i in topics_ebk]
    stems_lo = [stem_lines(i, ps) for i in all_lo]

    tfidf_vectorizer = TfidfVectorizer(stop_words=get_stopwords(),
                                       token_pattern=u'(?ui)\\b\\w*[a-zA-Z]+\\w*\\b'
                                       )

    tfidf_vectorizer.fit(all_lo)

    groupTopic_gisbok = gisbok.groupby("topic")
    groupTopic_ebk = ebk.groupby("Topic")

    embed_topic_gisbok = bow_topics(groupTopic_gisbok, tfidf_vectorizer)
    embed_topic_ebk = bow_topics(groupTopic_ebk, tfidf_vectorizer)

    embedded_gisbok = np.squeeze(np.asarray(embed_topic_gisbok["embedding"].to_list()))
    embedded_ebk = np.squeeze(np.asarray(embed_topic_ebk["embedding"].to_list()))
    similarity_score_matrix = embedded_gisbok.dot(embedded_ebk.T)
    index_very_similar = np.argwhere(
        similarity_score_matrix > 0.4)  # similarity_score_matrix is a symetic square matrix
    print(len(index_very_similar))
    set(index_very_similar[:, 0])
    pairs = set()
    for i in index_very_similar:
        pairs.add(embed_topic_gisbok["topic"].iloc[i[0]] + " ; " +embed_topic_ebk["topic"].iloc[i[1]])
    print("\n".join(pairs))

    print("topic no match in gisbok:")
    for i in range(len(embed_topic_gisbok)):
        if i not in set(index_very_similar[:,0]):
            print(embed_topic_gisbok["topic"].iloc[i])

    print("topic no match in ebk:")
    for i in range(len(embed_topic_ebk)):
        if i not in set(index_very_similar[:,1]):
            print(embed_topic_ebk["topic"].iloc[i])

    # nlp = spacy.load("en_core_web_lg")
    # gisbok_nlp = [nlp(i) for i in gisbok_docs]
    # ebk_nlp = [nlp(i) for i in ebk_docs]
    # for i, gis in enumerate(gisbok_nlp):
    #     for j, ebk in enumerate(ebk_nlp):
    #         if gis.similarity(ebk) >0.9:
    #             print(i,j)