import json
import logging
from datetime import datetime, date
from tempfile import mkdtemp
import boto3

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (TimeoutException, NoSuchElementException, 
                                        StaleElementReferenceException, NoAlertPresentException)
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


def str2date(date_str: str) -> date:
    """
    string으로 된 날짜 정보를 Date 객체로 변환
    :param date_str: string으로 저장된 날짜 정보
    :return: 날짜 정보가 담긴 Date 객체
    """
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()


def get_post_info(driver: webdriver.Chrome, wait: WebDriverWait, url: str):
    """
    주어진 URL의 게시글과 댓글을 수집
    :param driver: Selenium의 WebDriver, initialize_driver를 통해 생성된 인스턴스를 집어넣습니다.
    :param wait: driver를 통해 만들어진 Wait 인스턴스
    :param url: 게시글 URL
    :param page: logging용 page 정보
    :param post: logging용 post 정보
    :return: dict 형식의 post 정보
    """
    # 웹드라이버 설정
    driver.get(url)

    # 존재하지 않은 글의 alert 처리
    try:
        result = driver.switch_to.alert()
        result.accept()
    except NoAlertPresentException:
        pass
    else:
        logging.info('The post of URL "%s" is removed.', url)
        return {}

    page_dict = dict()

    try:
        # 제목 추출
        page_dict['title'] = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "post_subject"))
        ).text

        # 내용 추출
        page_dict['content'] = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "post_content"))
        ).text

        # 게시글 정보 추출 (조회수, 공감수, 작성자, 작성 날짜, 수정 날짜)
        page_dict['viewed'] = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "view_count"))
        ).text

        page_dict['liked'] = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "symph_count"))
        ).text

        page_dict['author'] = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "nickname"))
        ).text
    except TimeoutException:
        logging.error('Failed to get post info')
        return {}

    try:
        page_dict['viewed'] = int(page_dict['viewed'].replace(',', '')) if len(page_dict['viewed']) > 0 else 0
        page_dict['liked'] = int(page_dict['liked'][:-1].replace(',', '')) if len(page_dict['liked']) > 0 else 0
    except ValueError:
        if not isinstance(page_dict['viewed'], int):
            logging.error('Failed to convert "%s"', page_dict["viewed"])
        elif not isinstance(page_dict['liked'], int):
            logging.error('Failed to convert "%s"', page_dict["liked"])
        else:
            logging.error('Unknown error during converting to int')
        return {}

    try:
        date_text = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".post_author > span"))
        ).text.split('수정일 :')
    except TimeoutException:
        logging.error('Failed to find the created date')
        date_text = []

    page_dict['created_at'] = date_text[0].strip() if date_text else None
    page_dict['modified_at'] = date_text[1].strip() if len(date_text) > 1 else page_dict['created_at']
    url = driver.current_url
    page_dict['url'] = url

    # 댓글 추출
    comments = []
    try:
        comment_elms = wait.until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "comment_row"))
        )
    except TimeoutException:
        comment_elms = [] # 댓글 없음

    for comment in comment_elms:
        if 'blocked' not in comment.get_attribute('class'):
            comment_info = get_comment_info(comment)
            comments.append(comment_info)
    page_dict['comments'] = comments

    return page_dict


def get_comment_info(comment_elms: WebElement) -> dict:
    """
    주어진 댓글 Element 객체에서 댓글 정보 추출
    :param comment_element: 추출할 댓글을 가리키는 WebElement 객체
    :return: 댓글 추출 결과
    """
    comment_dict = {}
    try:
        comment_dict['content'] = comment_elms.find_element(
                By.CLASS_NAME, "comment_content"
            ).text
        comment_dict['liked'] = comment_elms.find_element(
                By.CLASS_NAME, "comment_symph"
            ).text
        comment_dict['author'] = comment_elms.find_element(
                By.CLASS_NAME, "nickname"
            ).text
        timestamp = comment_elms.find_element(
                By.CSS_SELECTOR, 'span.timestamp'
            ).get_attribute('innerHTML')
    except StaleElementReferenceException:
        logging.error('The comment element is stale.')
        return {}
    except NoSuchElementException:
        logging.error('Failed to read the comment data.')
        return {}

    comment_dict['liked'] = comment_dict['liked'].replace(',', '')
    comment_dict['liked'] = int(comment_dict['liked']) if len(comment_dict['liked']) > 0 else 0

    timestamp = timestamp.split('/ 수정일:')
    comment_dict['created_at'] = timestamp[0].strip() if timestamp else None
    comment_dict['modified_at'] = timestamp[1].strip() if len(timestamp) > 1 else comment_dict['created_at']

    return comment_dict


# sqs 클라이언트 생성
sqs = boto3.client('sqs')

def lambda_handler(event, context=None) -> dict:
    """
    lambda_handler
    """
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    urls = event.get('links', None)
    file_name = event.get('file_name', None)
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
            if line:
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
