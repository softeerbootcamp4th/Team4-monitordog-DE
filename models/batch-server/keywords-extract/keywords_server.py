import boto3
import json
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fast_sentence_transformers import FastSentenceTransformer as SentenceTransformer
from bareunpy import Tagger
import os
from flask import Flask, request, jsonify
import logging
from urllib import parse

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # 로그 포맷
    handlers=[
        logging.FileHandler("/var/log/flask.log"),  # 로그 파일 위치 설정
        logging.StreamHandler()  # 콘솔에 출력 설정
    ]
)

logger = logging.getLogger(__name__)


API_KEY = os.environ["API_KEY"]
tagger = Tagger(API_KEY, 'localhost', port=5757) # KPF에서 제공하는 바른 형태소 분석기

model = SentenceTransformer('/opt/flask/kpfSBERT-back', device='cpu')

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


def batch_inference(bucket_name, file_name):
    file_name = parse.unquote(file_name)
    logging.info("파라미터 확인, 버킷: " + bucket_name + ", 파일: " + file_name)

    local_file_path = "/tmp/local_file.jsonl"
    try:
        s3 = boto3.client('s3')
        # S3에서 파일 다운로드
        s3.download_file(bucket_name, file_name, local_file_path)
    except Exception as e:
        logging.info("파일 다운로드 실패", e.with_traceback())

    logging.info("jsonl inference 시작")
    line_num = 1
    # JSONL 파일 읽기 및 추론
    with open(local_file_path, 'r') as infile, open('/tmp/modified_file.jsonl', 'w') as outfile:
        for line in infile:
            line_num += 1
            record = json.loads(line)
            input_data = record['title'] + " " + record['content']

            keywords = keyword_ext(input_data)

            # 결과 추가
            record['keywords'] = keywords

            # 수정된 행 저장
            outfile.write(json.dumps(record, ensure_ascii=False) + '\n')
    logging.info("jsonl inference 종료")


    try:
        logging.info("파일 업로드")
        file_metadata = file_name.split("/")
        origin_file_name = file_metadata[-1]
        origin_file_path = file_metadata[-2]
        # 수정된 파일을 S3에 업로드
        s3.upload_file('/tmp/modified_file.jsonl', bucket_name, f"keywords/{origin_file_path}/{origin_file_name}")
    except Exception as e:

        logging.info("파일 업로드 실패", e.with_traceback())


@app.route('/keyword_extraction', methods=['POST'])
def extract_keywords():
    data = request.json

    # Lambda에서 전달된 매개변수
    bucket_name = data.get('bucket_name')
    file_name = data.get('file_name')

    if not bucket_name:
        return jsonify({'error': 'No bucket_name provided'}), 400

    if not file_name:
        return jsonify({'error': 'No file_name provided'}), 400

    batch_inference(bucket_name, file_name)
    return "batch process is called"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)