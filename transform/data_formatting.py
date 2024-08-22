import csv
import html
import json
import re
from datetime import datetime

import pandas as pd
from soynlp.normalizer import only_text, emoticon_normalize


# 텍스트 전처리용 regex 패턴
ALLOW_CHARS_PATTERN = re.compile('[^ㄱ-ㅎ가-힣a-zA-Z0-9 !\?\.,]')
URL_PATTERN = re.compile(r'''
    (https?://)?
    (?:www\.)?
    (?:[\w-]+\.)+
    [\w-]+
    (?:/[\w.-]*)*
    /?
    (?:\?[\w=&]*)?
    (?:\#[\w-]*)?
''', re.VERBOSE)
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
IMAGE_NUMBER_PATTERN = re.compile(r'\d(\s\d)+')

# 게시글 타입 enum
TYPE_POST = 0
TYPE_COMMENT = 1


# data sources
DATA_SOURCES = ['dcinside', 'naver', 'bobae', 'clien']


# 작성 날짜 변환 함수
def str2datetime_dcinside(text: str) -> datetime:
    """
    DCinside에서 수집한 작성 날짜를 datatime 타입으로 변환
    :param text: DCinside 작성 날짜 형식으로 적힌 날짜 문자열
    :return: datetime 형식으로 변환된 작성 날짜
    """
    try:
        created_at = datetime.strptime(text, "%Y.%m.%d %H:%M:%S")
    except ValueError:
        year = str(datetime.now().year)
        text = year + '.' + text
        created_at = datetime.strptime(text, "%Y.%m.%d %H:%M:%S")

    return created_at


def str2datetime_naver(text: str) -> datetime:
    """
    네이버 카페에서 수집한 작성 날짜를 datatime 타입으로 변환
    :param text: 네이버 카페 작성 날짜 형식으로 적힌 날짜 문자열
    :return: datetime 형식으로 변환된 작성 날짜
    """
    try:
        created_at = datetime.strptime(text, "%Y.%m.%d. %H:%M")
    except ValueError:
        year = str(datetime.now().year)
        text = year + '.' + text
        created_at = datetime.strptime(text, "%Y.%m.%d. %H:%M")

    return created_at


def str2datetime_bobae(text: str) -> datetime:
    """
    보배드림에서 수집한 작성 날짜를 datatime 타입으로 변환
    :param text: 보배드림 작성 날짜 형식으로 적힌 날짜 문자열
    :return: datetime 형식으로 변환된 작성 날짜
    """
    created_at = datetime.strptime(text, "%y.%m.%d  %H:%M")

    return created_at


def str2datetime_clien(text: str) -> datetime:
    """
    클리앙에서 수집한 작성 날짜를 datatime 타입으로 변환
    :param text: 클리앙 작성 날짜 형식으로 적힌 날짜 문자열
    :return: datetime 형식으로 변환된 작성 날짜
    """
    created_at = datetime.strptime(text, "%Y-%m-%d  %H:%M:%S")

    return created_at


# 텍스트 전처리
def preprocessing_text_dcinside(text: str) -> str:
    """
    DCinside 게시글의 제목과 본문 텍스트 전처리
    :param text: 전처리할 텍스트
    :return: 전처리된 텍스트
    """
    # url 제거
    text = URL_PATTERN.sub('', text)
    # 이메일 주소 제거
    text = EMAIL_PATTERN.sub('', text)
    # html 엔티티 인코딩
    text = html.unescape(text)
    # 특수문자 제거
    text = ALLOW_CHARS_PATTERN.sub(' ', text)
    # 이미지 표시 제거
    text = text.replace('이미지 순서 ON', '')
    text = text.replace('마우스 커서를 올리면 이미지 순서를 ON OFF 할 수 있습니다.', '')
    # 중복된 띄어쓰기 지우기
    text = re.sub(r'\s+', ' ', text)
    # 이미지 순서 표기 지우기
    text = IMAGE_NUMBER_PATTERN.sub('', text)
    # dc official app 제거
    words = [' dc App', ' dc official App', ' dc official app']
    for word in words:
        text = text.replace(word, '')

    text = only_text(text)
    text = emoticon_normalize(text)

    return text


def preprocessing_text_naver(text: str) -> str:
    """
    네이버 카페 게시글의 제목과 본문 텍스트 전처리
    :param text: 전처리할 텍스트
    :return: 전처리된 텍스트
    """
    # url 제거
    text = URL_PATTERN.sub('', text)
    # 이메일 주소 제거
    text = EMAIL_PATTERN.sub('', text)
    # html 엔티티 인코딩
    text = html.unescape(text)
    # 특수문자 제거
    text = ALLOW_CHARS_PATTERN.sub(' ', text)
    # 중복된 띄어쓰기 지우기
    text = re.sub(r'\s+', ' ', text)

    text = only_text(text)
    text = emoticon_normalize(text)

    return text


# 제목 전처리
def preprocessing_title_bobae(title: str) -> str:
    """
    보배드림 게시글의 제목을 추가로 텍스트 전처리
    :param title: 전처리할 텍스트
    :return: 전처리된 텍스트
    """
    title = only_text(title)
    title = emoticon_normalize(title)

    # 제목에 붙은 이미지 첨부 및 모바일 작성, 댓글 수 표시 제거
    title = re.sub(r'\(\d+\)( 이미지)?( 휴대전화)?$', '', title)

    # 중복된 띄어쓰기 지우기
    title = re.sub(r'\s+', ' ', title)

    return title


def preprocessing_title_clien(title: str) -> str:
    """
    클리앙 게시글의 제목을 추가로 텍스트 전처리
    :param title: 전처리할 텍스트
    :return: 전처리된 텍스트
    """
    title = only_text(title)
    title = emoticon_normalize(title)

    # 댓글 수 표시 제거
    title = re.sub(r' \d+$', '', title)
    # 중복된 띄어쓰기 지우기
    title = re.sub(r'\s+', ' ', title)

    return title


# 한국어 숫자표기 변환 함수
def str2num_naver(text: str) -> int:
    """
    네이버 카페 조회수와 추천수 표기를 정수로 변환
    :param text: 한국어 표기된 숫자
    :return: 정수로 변환된 숫자
    """
    base = 1
    base_match = re.search(r'[십백천]만|[십백천만]', text)

    if base_match is not None:
        base_text = base_match[0]
    else:
        base_text = ''

    if '십' in base_text:
        base = base * 10
    if '백' in base_text:
        base = base * 100
    if '천' in base_text:
        base = base * 1000
    if '만' in base_text:
        base = base * 10000

    num_text = re.sub(r'[십백천]만|[십백천만]', '', text).strip()
    return int(float(num_text) * base)


# comment 정보 변환
def get_comment_info_dcinside(comment_info: dict) -> list:
    """
    DCinside 게시글의 댓글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 댓글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_dcinside(comment_info['content'])
    created_at = str2datetime_dcinside(comment_info['created_at'])

    return [content,
            created_at,
            0, # viewed
            0, # liked
            TYPE_COMMENT, # post_typee
            0, # num_of_comments
            0, # seconds_per_comment
            ]


def get_comment_info_naver(comment_info: dict) -> list:
    """
    네이버 카페 게시글의 댓글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 댓글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_naver(comment_info['content'])
    created_at = str2datetime_naver(comment_info['created_at'])

    return [content,
            created_at,
            0, # viewed
            0, # liked
            TYPE_COMMENT, # post_type
            0, # num_of_comments
            0, # seconds_per_comment
            ]


def get_comment_info_bobae(comment_info: dict) -> list:
    """
    보배드림 게시글의 댓글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 댓글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_naver(comment_info['content'])
    created_at = str2datetime_bobae(comment_info['created_at'])

    return [content,
            created_at,
            0, # viewed
            0, # liked
            TYPE_COMMENT, # post_type
            0, # num_of_comments
            0, # seconds_per_comment
            ]


def get_comment_info_clien(comment_info: dict) -> list:
    """
    클리앙 게시글의 댓글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 댓글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_naver(comment_info['content'])
    created_at = str2datetime_clien(comment_info['created_at'])
    return [content,
            created_at,
            0, # viewed
            0, # liked
            TYPE_COMMENT, # post_type
            0, # num_of_comments
            0, # seconds_per_comment
            ]


def get_comment_info(comment_info: dict, data_source: str) -> list:
    """
    data source에 따라 댓글 정보 포멧을 통일화하는 함수
    :param post_info: 변환할 게시글 정보
    :param data_source: 게시글을 가져온 커뮤니티 이름, [dcinside, naver, bobae, clien] 중 하나 선택
    """
    if data_source == 'dcinside':
        return get_comment_info_dcinside(comment_info)
    elif data_source == 'naver':
        return get_comment_info_naver(comment_info)
    elif data_source == 'bobae':
        return get_comment_info_bobae(comment_info)
    elif data_source == 'clien':
        return get_comment_info_clien(comment_info)
    else:
        raise ValueError(f'Cannot use data source "{data_source}"')


# post 정보 변환
def get_post_info_dcinside(post_info: dict) -> list:
    """
    DCinside 게시글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 게시글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_dcinside(post_info['title'] + ' ' + post_info['content'])
    created_at = str2datetime_dcinside(post_info['created_at'])

    return [content,
            created_at,
            post_info['viewed'],
            post_info['liked'],
            TYPE_POST,
            post_info['num_of_comments'],
            0,
            ]


def get_post_info_naver(post_info: dict) -> list:
    """
    네이버 카페 게시글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 게시글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    content = preprocessing_text_naver(post_info['title'] + ' ' + post_info['content'])
    created_at = str2datetime_naver(post_info['created_at'])
    viewed = str2num_naver(post_info['viewed'].replace('조회', '').replace(',', ''))
    liked = str2num_naver(post_info['liked'].replace(',', '').strip())
    num_of_comments = str2num_naver(post_info['num_of_comments'].replace(',', '').strip())

    return [content,
            created_at,
            viewed,
            liked,
            TYPE_POST,
            num_of_comments,
            0, #seconds_per_comments
            ]


def get_post_info_bobae(post_info: dict) -> list:
    """
    보배드림 게시글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 게시글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    title = preprocessing_title_bobae(post_info['title'])
    content = preprocessing_text_naver(title + ' ' + post_info['content'])
    created_at = str2datetime_bobae(post_info['created_at'])
    viewed = int(post_info['viewed'].strip())
    liked = int(post_info['liked'].strip())
    num_of_comments = len(post_info['comments'])

    # post 정보 저장
    return [content,
            created_at,
            viewed,
            liked,
            TYPE_POST,
            num_of_comments,
            0, # seconds_per_comments
            ]


def get_post_info_clien(post_info: dict) -> list:
    """
    클리앙 게시글 정보를 정해진 포멧에 따라 변환
    :param comment_info: 게시글 정보가 들어있는 딕셔너리
    :return: 정해진 csv column 순서에 따라 배치된 리스트
    """
    title = preprocessing_title_clien(post_info['title'])
    content = preprocessing_text_naver(title + ' ' + post_info['content'])
    created_at = str2datetime_clien(post_info['created_at'])
    num_of_comments = len(post_info['comments'])

    return [content,
            created_at,
            post_info['viewed'],
            post_info['liked'],
            TYPE_POST,
            num_of_comments,
            0, # seconds_per_comments
            ]


def get_post_info(post_info: dict, data_source: str) -> list:
    """
    data source에 따라 게시글 정보 포멧을 통일화하는 함수
    :param post_info: 변환할 게시글 정보
    :param data_source: 게시글을 가져온 커뮤니티 이름, [dcinside, naver, bobae, clien] 중 하나 선택
    """
    if data_source == 'dcinside':
        return get_post_info_dcinside(post_info)
    elif data_source == 'naver':
        return get_post_info_naver(post_info)
    elif data_source == 'bobae':
        return get_post_info_bobae(post_info)
    elif data_source == 'clien':
        return get_post_info_clien(post_info)
    else:
        raise ValueError(f'Cannot use data source "{data_source}"')


def jsonl2csv(filename: str, data_source: str) -> None:
    """
    게시글 파일을 csv 형식으로 변환하고 filename.csv로 저장합니다..
    :param filename: csv 형식으로 변환할 jsonl 파일 이름
    :param data_source: 게시글을 가져온 커뮤니티, [dcinside, naver, bobae, clien] 중 하나를 선택
    """
    if data_source not in DATA_SOURCES:
        raise ValueError(f'Cannot use data source "{data_source}"')

    csv_f = open(filename + '.csv', 'w', encoding='utf-8', newline='\n')
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow(['content', 'created_at', 'viewed', 'liked', 'post_type', 'num_of_comments', 'seconds_per_comment'])

    with open(filename + '.jsonl', 'r', encoding='utf-8') as f:
        line = f.readline()
        while line is not None and len(line) > 0:
            post_info = json.loads(line)

            # 크롤링에 실패한 게시글 생략
            if len(post_info) == 0:
                line = f.readline()
                continue

            # comments 정보 수집
            if post_info['comments'] is None:
                post_info['comments'] = []
            for comment_info in post_info['comments']:
                if comment_info['created_at'].strip() == '': # 삭제된 댓글 무시
                    continue
                comment = get_comment_info(comment_info, data_source)
                last_comment_created_at = comment[1] # get created_at
                csv_writer.writerow(comment)

            post = get_post_info(post_info, data_source)

            # seconds_per_comment 계산
            if post[-2] > 0:
                seconds_per_comment = (last_comment_created_at - post[1]).seconds / float(post[-2])
            else:
                seconds_per_comment = 0.0
            post[-1] = seconds_per_comment

            # post 정보 저장
            csv_writer.writerow(post)

            line = f.readline()
    csv_f.close()


if __name__ == '__main__':
    jsonl2csv('grandeur_dc', 'dcinside')
    jsonl2csv('naver_cafe_아이오닉6 누수_2024-08-11_14-36-03', 'naver')
    jsonl2csv('bobae_아이오닉6_2024-08-20T00_00_00', 'bobae')
    jsonl2csv('clien_iccu', 'clien')
