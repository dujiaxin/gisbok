import pandas as pd
import json
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize
import scipy
import argparse



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


def bow_topics(x, tfidf_vectorizer):
    embeddedTopic = []
    for name, group in x:
        # print(name)
        # print(group)
        topics = group['learning_objective'].to_list()
        stemmed = [stem_lines(i, ps) for i in topics]
        embedded_gisbok = tfidf_vectorizer.transform([" ".join(stemmed)])
        embeddedTopic.append({"topic":name, "embedding": embedded_gisbok})
    return pd.DataFrame(embeddedTopic)


def bow_papers(x1,x2,ps, tfidf_vectorizer):
    stemmed = stem_lines(x1+x2, ps)
    embedded_gisbok = tfidf_vectorizer.transform([stemmed])
    return embedded_gisbok


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("echo")
    # args = parser.parse_args()
    # print(args.echo)
    gisbokraw = pd.read_csv('adjusted.csv')
    gisbok = gisbokraw.fillna(axis=1, method='ffill')
    topics_gisbok = gisbok["topic"].to_list()
    all_lo = gisbok["learning_objective"].to_list() + topics_gisbok

    # gisbok_json = json.load(gisbok.to_json(orient='records'))
    ps = PorterStemmer()
    # stems_gisbok = [stem_lines(i, ps) for i in topics_gisbok]
    stems_lo = [stem_lines(i, ps) for i in all_lo]

    tfidf_vectorizer = TfidfVectorizer(stop_words=get_stopwords())
    tfidf_vectorizer.fit(stems_lo)

    groupTopic_gisbok = gisbok.groupby("topic")
    embed_topic_gisbok = bow_topics(groupTopic_gisbok, tfidf_vectorizer)
    # ebk = pd.read_csv('en.csv', quotechar='@')
    # ebk['embedding'] = ebk.apply(lambda x: bow_papers(x['abstract'], x['PaperTitle'], ps, tfidf_vectorizer), axis=1)
    #
    # embedded_gisbok = scipy.sparse.vstack(embed_topic_gisbok["embedding"].to_list())
    # embedded_ebk = scipy.sparse.vstack(ebk["embedding"].to_list())
    # similarity_score_matrix = embedded_gisbok.dot(embedded_ebk.T)

    ebk=pd.read_pickle('embedding.pickle')
    embedded_ebk = scipy.sparse.vstack(ebk["embedding"].to_list())
    with open('gis_q.csv', 'r') as read_obj:
        list_of_rows = read_obj.read().splitlines()
        stems_q = [stem_lines(i, ps) for i in list_of_rows]
        embedding_q = tfidf_vectorizer.transform(stems_q)
        similarity_score_matrix = embedding_q.dot(embedded_ebk.T).toarray()
        index_sort = similarity_score_matrix.argsort(axis=-1)
        for i in range(similarity_score_matrix.shape[0]):
            print("search q:")
            print(list_of_rows[i])
            print("title:")
            paper_list_40 = []
            paper_list_0 = []
            for ii in range(similarity_score_matrix.shape[1]):
                if similarity_score_matrix[i][index_sort[i][-1 - ii]] > 0.4:
                    print(ebk["PaperTitle"].iloc[index_sort[i][-1 - ii]])
                    paper_list_40.append(ebk["PaperId"].iloc[index_sort[i][-1 - ii]])
                elif similarity_score_matrix[i][index_sort[i][-1 - ii]] > 0:
                    if ii > 5:
                        break
                    try:
                        print(ebk["PaperTitle"].iloc[index_sort[i][-1 - ii]].encode('utf-8').strip())
                    except:
                        pass
                    paper_list_0.append(ebk["PaperId"].iloc[index_sort[i][-1 - ii]])



