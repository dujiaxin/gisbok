import pandas as pd
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize
import scipy
import argparse
import multiprocessing
from gensim.models import KeyedVectors
from gensim.utils import tokenize

def get_stopwords():
    stopwords_file = './stopwords.txt'
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    with open(stopwords_file, 'r', encoding='utf-8') as f:
        custom_stopwords = set(f.read().splitlines())
    all_stopwords = default_stopwords | custom_stopwords
    return all_stopwords


def stem_lines(line, ps=PorterStemmer()):
    if len(line)<3:
        print('null line happends:')
        print(line)
        return 'null'
    tokens = word_tokenize(line)
    tokens = [word for word in tokens if len(word) > 1]
    stemmed_tokens = [ps.stem(i) for i in tokens]
    return ' '.join(stemmed_tokens)

def wv_gisbok(w, wv):
    tokens = tokenize(w)
    wvs = []
    for t in tokens:
        try:
            wvs.append(wv[t])
        except KeyError:
            print('KeyError')
            print(t)
    allnp = np.array(wvs).astype(np.float)
    return np.mean(allnp,axis=0)


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


def bow_papers(x1,x2,tfidf_vectorizer):
    stemmed = stem_lines(x1+x2)
    embedded_gisbok = tfidf_vectorizer.transform(stemmed)
    return embedded_gisbok


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("echo")
    # args = parser.parse_args()
    # print(args.echo)
    gisbokraw = pd.read_csv('adjusted.csv')
    gisbok = gisbokraw.fillna(axis=1, method='ffill')
    gisbok_topic = gisbok["topic"].drop_duplicates().to_list()
    gisbok_area = gisbok["area"].drop_duplicates().to_list()
    gisbok_theme = gisbok["theme"].drop_duplicates().to_list()
    gisbok_learning_objective = gisbok["learning_objective"].drop_duplicates().to_list()
    all_lo = gisbok_area + gisbok_theme + gisbok_topic + gisbok_learning_objective
    print("len(all_lo)")
    print(len(all_lo))
    # gisbok_json = json.load(gisbok.to_json(orient='records'))
    ps = PorterStemmer()
    # stems_gisbok = [stem_lines(i, ps) for i in topics_gisbok]
    stems_lo = [stem_lines(i, ps) for i in all_lo]

    tfidf_vectorizer = TfidfVectorizer(stop_words=get_stopwords())
    tfidf_vectorizer.fit(stems_lo)

    groupTopic_gisbok = gisbok.groupby("topic")
    # with Pool(cores) as p:
    #     sentences = p.map(stem_lines, list_ab)
    embed_topic_gisbok = bow_topics(groupTopic_gisbok, tfidf_vectorizer)
    embed_ontology_tfidf = tfidf_vectorizer.transform(stems_lo)
    print("embed_ontology_tfidf.shape")
    print(embed_ontology_tfidf.shape)
    wv = KeyedVectors.load("fastText_model_real")
    embed_ontology_fasttext = np.array([wv_gisbok(i,wv) for i in all_lo])
    embed_ontology_scibert = np.load("gisbok_scibert_embedding.npy")
    print(embed_ontology_fasttext.shape)
    print(embed_ontology_scibert.shape)
    embed_ontology = np.hstack((embed_ontology_tfidf.toarray(),
                               embed_ontology_fasttext,
                               embed_ontology_scibert))
    print("embed_ontology.shape")
    print(embed_ontology.shape)
    ebk = pd.read_csv('en.csv', quotechar='@')
    ebk['stems'] = ebk.apply(lambda x: stem_lines(x['abstract'] + x['PaperTitle']), axis=1)

    word_vectors = KeyedVectors.load("fastText_model_real")

    embedded_ebk_tfidf = (tfidf_vectorizer.transform(ebk['stems'].to_list())).toarray()
    embedded_ebk_fasttext = pd.read_pickle("fasttext_real_embedding.pickle")
    embedded_ebk_scibert = pd.read_pickle("scibert_embedding.pickle")
    print("embedded_ebk_tfidf.shape")
    print(embedded_ebk_tfidf.shape)
    embedded_ebk = np.hstack((embedded_ebk_tfidf,
                            np.array(embedded_ebk_fasttext["fasttext_embedding"].to_list()),
                             np.squeeze(np.array(embedded_ebk_scibert["scibert_embedding"].to_list()))
                             ))
    print("embedded_ebk.shape")
    print(embedded_ebk.shape)
    similarity_score_matrix = embed_ontology.dot(embedded_ebk.T)
    np.savez_compressed('similarity_score_matrix', a=similarity_score_matrix)
    smatrix = np.load('similarity_score_matrix.npz')
    s=smatrix['a']
    print("loaded similarity_score_matrix ok")
    index_sort = s.argsort(axis=-1)

    topic_paper_list = []
    for i in range(s.shape[0]):
        print("all_lo:")
        print(all_lo[i])
        print("title:")
        paper_list_400 = []
        paper_list_300 = []
        for ii in range(s.shape[1]):
            if s[i][index_sort[i][-1 - ii]] > 400:
                print(ebk["PaperTitle"].iloc[index_sort[i][-1 - ii]])
                paper_list_400.append(ebk["PaperId"].iloc[index_sort[i][-1 - ii]])
            elif s[i][index_sort[i][-1 - ii]] > 300:
                try:
                    paper_list_300.append(ebk["PaperId"].iloc[index_sort[i][-1 - ii]])
                except:
                    pass
            else:
                break

        topic_paper_list.append(
            {"all_lo":all_lo[i],"PaperIdList400":paper_list_400,"PaperIdList300":paper_list_300}
        )
    pd.DataFrame(topic_paper_list).to_pickle("topic_papers.pickle")


