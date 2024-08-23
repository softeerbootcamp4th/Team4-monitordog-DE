from math import log

import numpy as np
import pandas as pd
from scipy.stats import zscore
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


def weighted_distance(a, b):
    """
    희소 시계열 데이터의 거리 계산을 위해 두 점의 거리를 가중치를 취한 거리 함수 
    """
    return np.abs(a - b)


def dtw_similarity_score(a, b, scale_factor: float = 0.1, radius: int = 1) -> float:
    """
    정규화된 DTW 유사도 계산
    :param x: 그래프를 나타내는 1차원 리스트 (pd.Series)
    :param y: 그래프를 나타내는 1차원 리스트 (pd.Series), x와 길이가 동일해야 정확한 값이 나옴
    :param radius: Fast DTW 알고리즘에서 경로를 탐색할 범위
    :return: [0,1] 사이의 실수값, 1에 가까울수록 유사한 그래프
    """
    # 입력이 pandas Series인지 확인
    if isinstance(a, pd.Series):
        a = a.to_numpy()
    if isinstance(b, pd.Series):
        b = b.to_numpy()

    a_x = np.arange(len(a))[a != 0]
    b_x = np.arange(len(b))[b != 0]

    a_nonzero = a[a != 0]
    b_nonzero = b[b != 0]

    num_a_nonzero = len(a_nonzero)
    num_b_nonzero = len(b_nonzero)

    # 비영 영역이 없는 경우 처리
    if num_a_nonzero == 0 and num_b_nonzero == 0:
        return 0.0 # 아무런 신호가 없는 경우 무시하기 위해 0 반환 
    elif num_a_nonzero == 0 or num_b_nonzero == 0:
        return 0.0
    
    a_nonzero = zscore(a_nonzero)
    b_nonzero = zscore(b_nonzero)

    a_nonzero = np.vstack((a_x, a_nonzero)).T
    b_nonzero = np.vstack((b_x, b_nonzero)).T

    raw_distance, _ = fastdtw(a_nonzero, b_nonzero, radius=radius)
    print(raw_distance)

    # 정규화된 거리 계산 (0과 1 사이의 값)
    similarity_score = np.exp(-raw_distance * scale_factor)

    return similarity_score


if __name__ == '__main__':
    a = pd.Series([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3000, 6000, 6000, 6000, 6001, 15, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    b = pd.Series([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1500, 3000, 3000, 3000, 3001, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    print(dtw_similarity_score(a, b))