from gensim.models.fasttext import FastText
import pandas as pd
import spacy
import multiprocessing
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from gensim.models import Word2Vec
from spacy.attrs import IS_TITLE, IS_PUNCT

def token_list(s):
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(s)
    return [token.text for token in doc]



if __name__ == "__main__":
    nlp = English()
    tokenizer = nlp.Defaults.create_tokenizer(nlp)
    a = pd.read_csv("adjusted.csv").fillna(axis=1, method='ffill')
    set_a = set(a["area"].to_list() + a["theme"].to_list() + a["topic"].to_list() + a["learning_objective"].to_list())
    b = pd.read_csv("en.csv", quotechar='@')
    set_b = set(b["PaperTitle"].to_list() + b["abstract"].to_list())
    list_ab = list(set_a) + list(set_b)
    cores = multiprocessing.cpu_count()
    sentences = []
    # pool = multiprocessing.Pool(processes=cores)
    # sentences = pool.map(token_list, list_ab)
    for s in tokenizer.pipe(list_ab):
        sentences.append([t.text for t in s if not t.check_flag(IS_PUNCT)])
    # sentences = [tokenizer.pipe(s) for s in list_ab]
    print(len(sentences))
    # Train the FastText model


    # word2vec_model = Word2Vec(sentences, size=100, window=5, min_count=5, workers=cores, sg=0)
    # word2vec_model.wv.save_word2vec_format("word2vec_model", binary=True)  # binary format

    fastText_model = FastText(size=100, window=5, min_count=5, sentences=sentences, iter=5, workers=cores)
    fastText_model.wv.save_word2vec_format("fastText_model", binary=True)
