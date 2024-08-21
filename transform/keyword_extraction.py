"""
바른 형태소 분석기와 KPF-KeyBERT를 통한 핵심어 추출기

referenced by https://github.com/KPF-bigkinds/BIGKINDS-LAB/blob/main/KPF-KeyBERT/keyword_module.py
"""

import os
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

from bareunpy import Tagger


API_KEY = os.environ['KPF_API_KEY'] # KPF에서 제공하는 API_KEY
tagger = Tagger(API_KEY, 'localhost', 7575) # KPF에서 제공하는 바른 형태소분석기

model = SentenceTransformer('kpfSBERT')


def keyword_ext(text: str, top_n: int = 5, diversity: float = 0.2):
    """
    텍스트에서 핵심어를 추출하는 함수
    :param text: 핵심어를 찾을 텍스트
    :param top_n: 찾아낼 최대 핵심어 개수, 형태소 분석기의 명사 추출 결과에 따라 top_n보다 적을 수 있음
    :param diversity: 1에 가까워질수록 키워드간 유사성을 중점으로, 0에 가까울수록 텍스트와의 유사성을 중점으로 핵심어 선택
    """
    tokenized_doc = tagger.pos(text)
    tokenized_nouns = ' '.join([word[0] for word in tokenized_doc if word[1] == 'NNG' or word[1] == 'NNP'])

    n_gram_range = (1,1)

    if len(tokenized_nouns) <= 0:
        print('warning: Cannot find nouns in "' + text + '"')
        return []
    try:
        count = CountVectorizer(ngram_range=n_gram_range).fit([tokenized_nouns])
    except ValueError:
        print('warning: Cannot find nouns in "' + text + '"')
        print('  tokenized_nouns:', tokenized_nouns)
        return []
    candidates = count.get_feature_names_out()

    doc_embedding = model.encode([text])
    candidate_embeddings = model.encode(candidates)

    return mmr(doc_embedding, candidate_embeddings, candidates, top_n=top_n, diversity=diversity)


def mmr(doc_embedding, candidate_embeddings, words, top_n, diversity):
    """
    임베딩된 문서와 핵심어 후보를 바탕으로 핵심어를 골라내는 함수
    """
    # 단어와 문서간의 의미 유사도
    word_doc_similarity = cosine_similarity(candidate_embeddings, doc_embedding)
    # 단어간 의미 유사도
    word_similarity = cosine_similarity(candidate_embeddings)

    # 문서 내용을 가장 잘 대표하는 단어를 첫번째 핵심어로 결정
    keywords_idx = [np.argmax(word_doc_similarity)]
    candidates_idx = [i for i in range(len(words)) if i != keywords_idx[0]]

    for _ in range(top_n - 1):
        if len(candidates_idx) <= 0:
            break
        candidate_similarities = word_doc_similarity[candidates_idx, :]
        target_similarities = np.max(word_similarity[candidates_idx][:, keywords_idx], axis=1)

        # MMR을 계산
        mmr = (1-diversity) * candidate_similarities - diversity * target_similarities.reshape(-1, 1)
        mmr_idx = candidates_idx[np.argmax(mmr)]

        # keywords & candidates를 업데이트
        keywords_idx.append(mmr_idx)
        candidates_idx.remove(mmr_idx)

    return [words[idx] for idx in keywords_idx]
