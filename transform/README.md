# /transform

크롤링된 게시글 포멧 통일화, 감정 분석, 키워드 추출 등 데이터 변환

## /trasnform/formatter
각 커뮤니티의 게시판 형식이 달라 크롤링된 데이터도 같은 항목에 다른 포멧 또는 다른 타입 값이 들어있습니다.
formatter는 이렇게 다른 데이터 형식을 하나로 통일하는 Lambda 입니다.

## issue_score.py
특정 화제가 커뮤니티에 퍼져있다면 특정 하나의 글이 댓글과 추천을 많이 받거나, 같은 주제의 글이 짧은 시간 내에 올라옵니다.
이슈화 점수는 이러한 활동은 간단히 점수화하는 것을 목표로 만들어졌습니다.
이후 과거 사건의 이슈화 흐름과 비교해 현재 이슈화 흐름에 대해 인사이트를 제공하기 위해 DTW 유사도 점수까지 계산합니다.

## keyword_extraction.py
커뮤니티 글의 내용을 한눈에 알 수 있도록 글의 내용을 잘 담고 있는 키워드를 추출합니다.
키워드 추출은 [바른 형태소 분석기](https://bareun.ai/)와 [kpfSBERT](https://github.com/KPFBERT/kpfSBERT?tab=readme-ov-file)를 통해 이루어집니다.

## sentiment_analysis.py
커뮤니티 글에 얼마나 분노가 담겨있는지를 알아보기 위해 부정적 또는 형오 내용 정도를 점수화합니다.
모델은 [smilegate-ai/kor_unsmile](https://huggingface.co/smilegate-ai/kor_unsmile)을 사용하였습니다.