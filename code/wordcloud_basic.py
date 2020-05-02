import pandas as pd

import nltk
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize

df = pd.read_csv('code/en.csv', quotechar='@')
all = df['abstract'].to_list() + df['PaperTitle'].to_list()

word = []
titles = df['PaperTitle'].to_list()
title = []
for i in titles:
    tokens = word_tokenize(i)
    for ii in tokens:
        title.append(ii)


for i in all:
    tokens = word_tokenize(i)
    for ii in tokens:
        word.append(ii)

import pandas as pd

df = pd.read_csv('code/en.csv', quotechar='@')
all = df['abstract'].to_list() + df['PaperTitle'].to_list()
word = []
import nltk
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize

for i in all:
    tokens = word_tokenize(i)
    for ii in tokens:
        word.append(ii)

import string

stopwords = nltk.corpus.stopwords.words('english') + [punc for punc in string.punctuation]
from collections import Counter

word_could_dict = Counter(word)
from wordcloud import WordCloud

wordcloud = WordCloud(width=1000, height=500).generate_from_frequencies(word_could_dict)
wordcloud.show()
wordcloud.to_file("word_cloud.png")
words = [w for w in word if w not in stopwords]
words = [w for w in words if w.lower() not in stopwords]
word_could_dict = Counter(words)
wordcloud = WordCloud(width=1000, height=500, background_color='white').generate_from_frequencies(word_could_dict)
wordcloud.to_image('wordcloud.png')
wordcloud.to_file('wordcloud.png')

