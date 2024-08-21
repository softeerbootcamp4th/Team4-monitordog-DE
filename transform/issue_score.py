from math import log

import pandas as pd
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw


def get_issue_score(viewed: float, liked: float, num_of_comments: float) -> float:
    """
    특정 시간대의 게시글들의 이슈화 정도를 점수화
    :param viewed: 특정 시간대 내 게시글들의 조회수 총합
    :param liked: 특정 시간대 내 추천 수 총합
    :param num_of_comments: 특정 시간대 내 댓글 수 총합
    :return: 특정 시간대에서의 이슈화 점수
    """
    return viewed + log(1 + liked) + log(1 + num_of_comments)


def dtw_similarity_score(x: pd.Series, y: pd.Series, radius: int = 1) -> float:
    """
    정규화된 DTW 유사도 계산
    :param x: 그래프를 나타내는 1차원 리스트 (pd.Series)
    :param y: 그래프를 나타내는 1차원 리스트 (pd.Series), x와 길이가 동일해야 정확한 값이 나옴
    :param radius: Fast DTW 알고리즘에서 경로를 탐색할 범위
    :return: [0,1] 사이의 실수값, 1에 가까울수록 유사한 그래프
    """
    # 입력이 pandas Series인지 확인
    if not isinstance(x, pd.Series) or not isinstance(y, pd.Series):
        raise TypeError("입력은 pandas Series 형태여야 합니다.")
    
    # fastdtw 사용
    raw_distance, _ = fastdtw(list(x.items()), list(y.items()), dist=euclidean, radius=radius)
    
    # 정규화를 위한 최대 가능 거리 계산
    max_seq = max(x.max(), y.max())
    min_seq = min(x.min(), y.min())
    max_distance = abs(max_seq - min_seq) * max(len(x), len(y))
    
    # 정규화된 거리 계산 (0과 1 사이의 값)
    normalized_distance = raw_distance / max_distance
    
    # 유사도 점수 계산 (1에 가까울수록 유사함)
    similarity_score = 1 - normalized_distance
    
    return similarity_score