import pandas as pd
import spacy
import multiprocessing
from multiprocessing import Pool
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from spacy.attrs import IS_TITLE, IS_PUNCT
from gensim.models import KeyedVectors,FastText,Word2Vec
from gensim.test.utils import datapath
import numpy as np
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize


def stem_lines(line, ps=PorterStemmer()):
    if len(line)<3:
        print(line)
        return 'null'
    tokens = word_tokenize(line)
    tokens = [word for word in tokens if len(word) > 1]
    stemmed_tokens = [ps.stem(i) for i in tokens]
    return ' '.join(stemmed_tokens)


def wv_papers(x1,x2, wv):
    tokens = stem_lines(x1+x2).split(' ')
    wvs = []
    for t in tokens:
        try:
            wvs.append(wv[t])
        except KeyError:
            print(t)
    allnp = np.array(wvs).astype(np.float)
    return np.mean(allnp,axis=0)


if __name__ == "__main__":
    # nlp = English()
    # tokenizer = nlp.Defaults.create_tokenizer(nlp)
    a = pd.read_csv("adjusted.csv").fillna(axis=1, method='ffill')
    #set_a = set(a["area"].to_list() + a["theme"].to_list() + a["topic"].to_list() + a["learning_objective"].to_list())
    alist = a.apply(lambda row: row["area"] + ' ' + row["theme"] + ' ' + row["topic"] + ' ' + row["learning_objective"],
                    axis=1)
    set_a = set(alist.to_list())
    b = pd.read_csv("en.csv", quotechar='@')
    set_b = set(b["PaperTitle"].to_list() + b["abstract"].to_list())
    list_ab = list(set_a) + list(set_b)
    cores = multiprocessing.cpu_count()
    ps = PorterStemmer()
    with Pool(cores) as p:
        sentences = p.map(stem_lines, list_ab)
    # sentences = [stem_lines(i) for i in list_ab]
    # for s in tokenizer.pipe(list_ab):
    #     sentences.append([t.text for t in s if not t.check_flag(IS_PUNCT)])

    print("Train the FastText model")
    # word2vec_model = Word2Vec(sentences, size=100, window=5, min_count=5, workers=cores, sg=0)
    # word2vec_model.wv.save_word2vec_format("word2vec_model", binary=True)  # binary format
    print(sentences[0])
    fastText_model = FastText(size=100, window=5, min_count=1, sentences=sentences, iter=5, workers=cores)
    fastText_model.wv.save("fastText_model")
    print("finish training")
    word_vectors = fastText_model.wv  # KeyedVectors.load_word2vec_format("fastText_model")

    b['embedding'] = b.apply(lambda x: wv_papers(x['abstract'], x['PaperTitle'], word_vectors), axis=1)
    print(b.head())
    print(len(b))
    b.to_pickle("fasttext_embedding.pickle")

