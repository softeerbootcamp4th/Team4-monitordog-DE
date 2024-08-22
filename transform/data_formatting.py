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


# jsonl 변환 함수
def jsonl2csv(filename: str, data_source: str) -> None:
    """
    data source에 따라 데이터 포멧을 통일화하는 함수
    :param filename: 변환할 게시글 데이터 파일의 이름
    :param data_source: 게시글을 가져온 커뮤니티 이름, [dcinside, naver, bobae, clien] 중 하나 선택
    """
    if data_source == 'dcinside':
        jsonl2csv_dcinside(filename)
    elif data_source == 'naver':
        jsonl2csv_naver(filename)
    elif data_source == 'bobae':
        jsonl2csv_bobae(filename)
    elif data_source == 'clien':
        jsonl2csv_clien(filename)
    else:
        raise ValueError(f'Cannot use data source "{data_source}"')


def jsonl2csv_dcinside(filename: str) -> None:
    """
    DCinside 게시글 파일을 csv 형식으로 변환하고 filename.csv로 저장합니다.
    :param filename: csv 형식으로 변환할 jsonl 파일 이름
    """
    csv_f = open(filename + '.csv', 'w', encoding='utf-8', newline='\n')
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow(['content', 'created_at', 'viewed', 'liked', 'post_type', 'num_of_comments', 'seconds_per_comment'])

    with open(filename + '.jsonl', 'r', encoding='utf-8') as f:
        line = f.readline()
        while line is not None and len(line) > 0:
            post_info = json.loads(line)
            post_created_at = str2datetime_dcinside(post_info['created_at'])

            if post_info['comments'] is None:
                post_info['comments'] = []
            for comment_info in post_info['comments']:
                if comment_info['created_at'].strip() == '': # 삭제된 댓글 무시
                    continue
                created_at = str2datetime_dcinside(comment_info['created_at'])
                csv_writer.writerow([preprocessing_text_dcinside(comment_info['content']),
                                     created_at,
                                     0,
                                     0,
                                     TYPE_COMMENT,
                                     0,
                                     0,
                                     ])

            if post_info['num_of_comments'] > 0:
                seconds_per_comments = (created_at - post_created_at).seconds / float(post_info['num_of_comments'])
            else:
                seconds_per_comments = 0.0

            csv_writer.writerow([preprocessing_text_dcinside(post_info['title'] + ' ' + post_info['content']),
                                 post_created_at,
                                 post_info['viewed'],
                                 post_info['liked'],
                                 TYPE_POST,
                                 post_info['num_of_comments'],
                                 seconds_per_comments,
                                 ])
            line = f.readline()
    csv_f.close()


def jsonl2csv_naver(filename: str) -> None:
    """
    네이버 카페 게시글 파일을 csv 형식으로 변환하고 filename.csv로 저장합니다..
    :param filename: csv 형식으로 변환할 jsonl 파일 이름
    """
    csv_f = open(filename + '.csv', 'w', encoding='utf-8', newline='\n')
    csv_writer = csv.writer(csv_f)
    csv_writer.writerow(['content', 'created_at', 'viewed', 'liked', 'post_type', 'num_of_comments', 'seconds_per_comment'])

    with open(filename + '.jsonl', 'r', encoding='utf-8') as f:
        line = f.readline()
        while line is not None and len(line) > 0:
            post_info = json.loads(line)
            post_created_at = str2datetime_naver(post_info['created_at'])

            if post_info['comments'] is None:
                post_info['comments'] = []
            for comment_info in post_info['comments']:
                if comment_info['created_at'].strip() == '': # 삭제된 댓글 무시
                    continue
                created_at = str2datetime_naver(comment_info['created_at'])
                csv_writer.writerow([preprocessing_text_naver(comment_info['content']),
                                     created_at,
                                     0,
                                     0,
                                     TYPE_COMMENT,
                                     0,
                                     0,
                                     ])

            # post 정보 수집
            post_info['viewed'] = str2num_naver(post_info['viewed'].replace('조회', '').replace(',', ''))
            post_info['liked'] = str2num_naver(post_info['liked'].replace(',', '').strip())
            post_info['num_of_comments'] = str2num_naver(post_info['num_of_comments'].replace(',', '').strip()) 

            if post_info['num_of_comments'] > 0:
                seconds_per_comments = (created_at - post_created_at).seconds / float(post_info['num_of_comments'])
            else:
                seconds_per_comments = 0.0

            csv_writer.writerow([preprocessing_text_naver(post_info['title'] + ' ' + post_info['content']),
                                 post_created_at,
                                 post_info['viewed'],
                                 post_info['liked'],
                                 TYPE_POST,
                                 post_info['num_of_comments'],
                                 seconds_per_comments,
                                 ])
            line = f.readline()
    csv_f.close()


def jsonl2csv_bobae(filename: str) -> None:
    """
    보배드림 게시글 파일을 csv 형식으로 변환하고 filename.csv로 저장합니다..
    :param filename: csv 형식으로 변환할 jsonl 파일 이름
    """
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
                created_at = str2datetime_bobae(comment_info['created_at'])
                csv_writer.writerow([preprocessing_text_naver(comment_info['content']),
                                     created_at,
                                     0,
                                     0,
                                     TYPE_COMMENT,
                                     0,
                                     0,
                                     ])
                
            # post 정보 전처리
            post_info['title'] = preprocessing_title_bobae(post_info['title'])
            post_info['created_at'] = str2datetime_bobae(post_info['created_at'])
            post_info['viewed'] = int(post_info['viewed'].strip())
            post_info['liked'] = int(post_info['liked'].strip())
            post_info['num_of_comments'] = len(post_info['comments']) 

            if post_info['num_of_comments'] > 0:
                seconds_per_comments = (created_at - post_info['created_at']).seconds / float(post_info['num_of_comments'])
            else:
                seconds_per_comments = 0.0

            # post 정보 저장
            csv_writer.writerow([preprocessing_text_naver(post_info['title'] + ' ' + post_info['content']),
                                 post_info['created_at'],
                                 post_info['viewed'],
                                 post_info['liked'],
                                 TYPE_POST,
                                 post_info['num_of_comments'],
                                 seconds_per_comments,
                                 ])
            line = f.readline()
    csv_f.close()


def jsonl2csv_clien(filename: str) -> None:
    """
    클리앙 게시글 파일을 csv 형식으로 변환하고 filename.csv로 저장합니다..
    :param filename: csv 형식으로 변환할 jsonl 파일 이름
    """
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
                created_at = str2datetime_clien(comment_info['created_at'])
                csv_writer.writerow([preprocessing_text_naver(comment_info['content']),
                                     created_at,
                                     0,
                                     0,
                                     TYPE_COMMENT,
                                     0,
                                     0,
                                     ])
                
            # post 정보 전처리
            post_info['title'] = preprocessing_title_clien(post_info['title'])
            post_info['created_at'] = str2datetime_clien(post_info['created_at'])
            post_info['num_of_comments'] = len(post_info['comments']) 

            if post_info['num_of_comments'] > 0:
                seconds_per_comments = (created_at - post_info['created_at']).seconds / float(post_info['num_of_comments'])
            else:
                seconds_per_comments = 0.0

            # post 정보 저장
            csv_writer.writerow([preprocessing_text_naver(post_info['title'] + ' ' + post_info['content']),
                                 post_info['created_at'],
                                 post_info['viewed'],
                                 post_info['liked'],
                                 TYPE_POST,
                                 post_info['num_of_comments'],
                                 seconds_per_comments,
                                 ])
            line = f.readline()
    csv_f.close()
