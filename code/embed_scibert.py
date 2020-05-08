from transformers import *
import pandas as pd
import torch
import json
import numpy as np
from sklearn.manifold import TSNE

def sb_papers(x1,x2, tokenizer, model):
    rawtext = x1+". "+x2.replace("Abstract", "")
    indexed_tokens = torch.tensor(
        tokenizer.encode(rawtext, add_special_tokens=True, max_length=512)).unsqueeze(
        0)  # Batch size 1
    outputs = model(indexed_tokens.to("cuda:1"))
    embedding = outputs[0][:, 0, :]
    return embedding.cpu().numpy()


def main():
    with torch.no_grad():
        device = "cuda:1"
        tokenizer = AutoTokenizer.from_pretrained('allenai/scibert_scivocab_uncased')
        model = AutoModel.from_pretrained('allenai/scibert_scivocab_uncased')
        model.to(device)
        model.eval()
        metadata = pd.read_pickle("fasttext_embedding.pickle")
        metadata['scibert_embedding'] = metadata.apply(lambda x: sb_papers(x['abstract'], x['PaperTitle'], tokenizer, model), axis=1)
        b=metadata[["PaperId","PaperTitle","embedding","scibert_embedding"]]
        b.to_pickle("scibert_embedding.pickle")


if __name__ == '__main__':
    main()