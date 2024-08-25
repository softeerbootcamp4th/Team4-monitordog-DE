from flask import Flask, request, jsonify
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
from bareunpy import Tagger
import os

API_KEY = os.environ["API_KEY"]
tagger = Tagger(API_KEY, 'localhost', port=5757) # KPF에서 제공하는 바른 형태소 분석기

model = SentenceTransformer('/opt/flask/kpfSBERT')

app = Flask(__name__)

def keyword_ext(text, top_n=5, diversity=0.2):
    tokenized_doc = tagger.pos(text)
    tokenized_nouns = ' '.join([word[0] for word in tokenized_doc if word[1] == 'NNG' or word[1] == 'NNP'])

    n_gram_range = (1,1)

    if len(tokenized_nouns) <= 0:
        return []

    try:
        count = CountVectorizer(ngram_range=n_gram_range).fit([tokenized_nouns])
    except ValueError:
        return []
    
    candidates = count.get_feature_names_out()

    doc_embedding = model.encode([text])
    candidate_embeddings = model.encode(candidates)

    return mmr(doc_embedding, candidate_embeddings, candidates, top_n=top_n, diversity=diversity)

def mmr(doc_embedding, candidate_embeddings, words, top_n, diversity):
    word_doc_similarity = cosine_similarity(candidate_embeddings, doc_embedding)
    word_similarity = cosine_similarity(candidate_embeddings)

    keywords_idx = [np.argmax(word_doc_similarity)]
    candidates_idx = [i for i in range(len(words)) if i != keywords_idx[0]]

    for _ in range(top_n - 1):
        if len(candidates_idx) <= 0:
            break
        
        candidate_similarities = word_doc_similarity[candidates_idx, :]
        target_similarities = np.max(word_similarity[candidates_idx][:, keywords_idx], axis=1)

        mmr = (1-diversity) * candidate_similarities - diversity * target_similarities.reshape(-1, 1)
        mmr_idx = candidates_idx[np.argmax(mmr)]

        keywords_idx.append(mmr_idx)
        candidates_idx.remove(mmr_idx)

    return [words[idx] for idx in keywords_idx]

@app.route('/keyword_extraction', methods=['POST'])
def extract_keywords():
    data = request.json
    text = data.get('text')
    top_n = data.get('top_n', 5)
    diversity = data.get('diversity', 0.2)
    
    if not text:
        return jsonify({'error': 'No text provided'}), 400
    
    keywords = keyword_ext(text, top_n, diversity)
    return jsonify({'keywords': keywords})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
