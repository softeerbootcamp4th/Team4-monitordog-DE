from flask import Flask, request, jsonify
from transformers import TextClassificationPipeline, BertForSequenceClassification, AutoTokenizer


app = Flask(__name__)


class SentimentModel(object):
    """
    huggingfaces 감정 분석 모델 클래스
    """
    def __init__(self, model_name: str = 'smilegate-ai/kor_unsmile', **configs):
        """
        감정 분석 모델을 사용하기 위한 초기화 함수
        :param model_name: huggingfaces 모델 이름
        """
        self.model_name = model_name
        self.model = BertForSequenceClassification.from_pretrained(model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, clean_up_tokenization_spaces=False)
        self.pipe = TextClassificationPipeline(
            model = self.model,
            tokenizer = self.tokenizer,
            device = -1,   # cpu: -1, gpu: gpu number
            top_k = None, # 모든 class 점수 반환
            function_to_apply = 'sigmoid'
        )
        if len(configs) > 0:
            self.configs = configs
        else:
            self.configs = {
                'padding': True, 
                'truncation': True, 
                'max_length': 300,
            }


    def get_sentiment_score(self, text: str) -> float:
        """
        감정 분석 모델을 통해 텍스트의 부정적 감정을 점수화
        :param pipe: 감정 분석을 수행하는 모델 파이프라인
        :param text: 감정 분석 대상 텍스트
        :return: 텍스트 내용에 담긴 부정적 감정의 정도를 점수화, 1에 가까울수록 부정적
        """
        results = self.pipe(text, **self.configs)
        # 결과 중 clean 텍스트일 확률 고르기
        labels = list(map(lambda x: x['label'], results[0]))
        score = results[0][labels.index('clean')]['score']

        return 1.0 - score # clean 텍스트 확률을 욕설이나 비방글일 확률로 변환


@app.route('/sentiment_analysis', methods=['POST'])
def extract_keywords():
    data = request.json
    text = data.get('text')

    if not text:
        return jsonify({'error': 'No text provided'}), 400
    
    score = model.get_sentiment_score(text)
    return jsonify({'score': score})


if __name__ == '__main__':
    model = SentimentModel()
    app.run(host='0.0.0.0', port=8080)