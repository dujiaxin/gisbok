import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from os import listdir
from os.path import isfile, join
import pandas as pd

def get_all_topics():
    url_base = "https://gistbok.ucgis.org/all-topics?term_node_tid_depth=All&page="
    url_list = [url_base + str(i) for i in range(0, 30)]

    topic_set = set()
    for i, url in tqdm(enumerate(url_list)):
        r = requests.get(url)
        to_file_path = "./data/topics_page_" + str(i) + ".html"
        with open(to_file_path, "w") as f:
            f.write(r.text)
        # read a html file and extract what you need using BeautifulSoup
        with open(to_file_path, "r") as f:
            html = f.read()
        soup = BeautifulSoup(html, "html.parser")
        hrefs = soup.find_all('a')
        for href in hrefs:
            href_text = href.get('href')
            if href_text.startswith('/bok-topics'):
                topic_set.add(href_text.replace("/bok-topics/", ""))
        return topic_set


def save_urls(topic_set):
    for topic in tqdm(topic_set):
        r = requests.get("https://gistbok.ucgis.org/bok-topics/" + topic)
        to_file_path = "./data/gisbok_topic_" + topic + ".html"
        with open(to_file_path, "w", encoding='utf-8') as f:
            f.write(r.text)


if __name__ == "__main__":
    # save the web page to a html file
    # topic_set = get_all_topics()
    onlyfiles = [f for f in listdir("./data") if isfile(join("./data", f))]
    links={}
    # read a html file and extract what you need using BeautifulSoup
    for file in onlyfiles:
        if file.startswith("gisbok_topic"):
            with open("./data/"+file, "r", encoding='utf-8') as f:
                html = f.read()
            soup = BeautifulSoup(html, "html.parser")
            learning_objectives = soup.find(class_="field-name-field-learning-objectives").find_all('li')
            for lo in learning_objectives:
                print(lo.text)
            title = soup.find(id='page-title').string
            related_topics = soup.find(id="related-topics")
            if related_topics == None:
                continue
            rs = []
            for r in related_topics.find_all('a'):
                rs.append(r.string)
            links[title]=rs
    df = pd.DataFrame.from_dict(links)
    df.to_csv("./data/links.csv")
