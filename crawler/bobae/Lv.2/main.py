import json
import logging
import time
from datetime import datetime
from tempfile import mkdtemp

import boto3

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (TimeoutException, NoSuchElementException, StaleElementReferenceException, NoAlertPresentException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def initialize_driver() -> webdriver.Chrome:
    """
    AWS Lambda 환경에서도 Selenium이 구동되도록 Chrome 초기화
    :return: AWS Lambda 환경에서도 돌아가는 Chrome의 WebDriver 객체
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log"
    )

    driver = webdriver.Chrome(
        service=service,
        options=chrome_options
    )
    return driver


def str2date(date_str: str):
    """
    string으로 된 날짜 정보를 Date 객체로 변환
    :param date_str: string으로 저장된 날짜 정보
    :return: 날짜 정보가 담긴 Date 객체
    """
    return datetime.strptime(date_str, "%y. %m. %d").date()


def get_post_info(driver: webdriver.Chrome, wait: WebDriverWait, url: str) -> dict[str, str]:
    """
    주어진 URL의 게시글과 댓글을 수집
    :param driver: Selenium의 WebDriver, initialize_driver를 통해 생성된 인스턴스를 집어넣습니다.
    :param wait: driver를 통해 만들어진 Wait 인스턴스
    :param url: 게시글 URL
    :param page: logging용 page 정보
    :param post: logging용 post 정보
    :return: dict 형식의 post 정보
    """
    driver.get(url)

    # 존재하지 않은 글의 alert 처리
    try:
        result = driver.switch_to.alert()
        result.accept()
    except NoAlertPresentException:
        pass
    else:
        logging.info('The post of URL "%s" is removed.')
        return {}

    # 게시글 제목 추출
    try:
        title = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "header.article-tit > div.title")
                )
            ).text
        content = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.article-body")
                )
            ).text
        author = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "header.article-tit > div.util2 > div.info > span")
                )
            ).text
        created_at = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "header.article-tit > div.util > time")
                )
            ).text
        viewed = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "header.article-tit > div.util > span.data4")
                )
            ).text.split()[1]
        liked = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "header.article-tit > div.util > span.data3")
                )
            ).text.split()[1]
    except TimeoutException:
        logging.error('Failed to get post info: page %d, post %d')
        return {}

    # 댓글 추출
    comments = []
    try:
        comment_elements = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "div.commentListArea > div.cmtList > dl")
                )
            )
    except TimeoutException:
        comment_elements = [] # 댓글이 없는 경우

    for comment in comment_elements:
        comment_info = get_comment_info(comment)

        # 대댓글 확인 및 추출
        children = []
        try:
            reply_button = comment.find_element(By.CSS_SELECTOR, "dd > div.cmt_reply > a.reply")
            reply_button.click()
            time.sleep(1)  # 대댓글 로딩 대기

            child_comments = comment.find_elements(By.CSS_SELECTOR, "dd > div.replyList > dl")
        except TimeoutException:
            pass  # 대댓글이 없는 경우
        else:
            for child in child_comments:
                child_info = get_comment_info(child)
                children.append(child_info)

        comments.append({
            "comment": comment_info,
            "children": children
        })
    
    url = driver.current_url

    post_data = {
        "title": title,
        "content": content,
        "author": author,
        "created_at": created_at,
        "viewed": viewed,
        "num_of_comments": len(comments),
        "liked": liked,
        "comments": comments,
        "url": url
    }

    return post_data


def get_comment_info(comment_element: WebElement) -> dict:
    """
    주어진 댓글 Element 객체에서 댓글 정보 추출
    :param comment_element: 추출할 댓글을 가리키는 WebElement 객체
    :return: 댓글 추출 결과
    """
    try:
        author = comment_element.find_element(By.CSS_SELECTOR, "dt > span.cmt_nickname").text
        content = comment_element.find_element(By.CSS_SELECTOR, "dd > p").text
        created_at = comment_element.find_element(By.CSS_SELECTOR, "dt > span.date").text
    except NoSuchElementException:
        logging.error("Error: Can't find data from the given comment element")
    except StaleElementReferenceException:
        logging.error("Error: The given comment element is stale.")
        return {}

    return {
        "author": author,
        "content": content,
        "created_at": created_at
    }


# sqs 클라이언트 생성
sqs = boto3.client('sqs')

def lambda_handler(event, context=None):
    """
    lambda_handler
    """
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    urls = event.get('links', None)
    queue_url = event.get('queue_url', None)

    # 웹드라이버 설정
    driver = initialize_driver()
    wait = WebDriverWait(driver, 10)

    results = []
    for url in urls:
        results.append(get_post_info(driver, wait, url))

    # 브라우저 종료
    driver.quit()

    try:
        messages = ""
        # 파일 읽기
        for line in results:
            # 각 라인(메시지)을 읽어서 SQS로 전송
            json_line = json.dumps(line, ensure_ascii=False)  # 공백 제거
            messages += f"{json_line}\n"
                
        # SQS로 메시지 전송
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=messages
        )
        logging.info(f"Message sent to SQS: {response['MessageId']}")
    except Exception as e:
        logging.error(f"Error occurred while sending message to SQS: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Error occurred while sending message to SQS")
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Messages sent to SQS successfully')
    }
