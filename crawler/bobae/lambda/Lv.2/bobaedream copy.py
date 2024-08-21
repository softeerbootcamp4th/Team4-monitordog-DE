import json
import logging
import time
from datetime import datetime
from tempfile import mkdtemp

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


def str2date(date_str: str):
    """
    string으로 된 날짜 정보를 Date 객체로 변환
    :param date_str: string으로 저장된 날짜 정보
    :return: 날짜 정보가 담긴 Date 객체
    """
    return datetime.strptime(date_str, "%y. %m. %d").date()


def get_post_urls(driver: webdriver.Chrome, wait: WebDriverWait,
                  keyword: str, start_datetime: datetime) -> list[str]:
    """
    keyword 검색 결과에 해당하는 posts의 URL을 가져온다.
    :param driver: Selenium의 WebDriver, initialize_driver를 통해 생성된 인스턴스를 집어넣습니다.
    :param wait: driver를 통해 만들어진 Wait 인스턴스
    :param keyword: 검색 키워드
    :param start_date: 가져올 게시글의 시작 날짜
    """
    start_date = start_datetime.date()

    urls = []
    page_post_list = []

     # 검색 URL
    search_url = "https://m.bobaedream.co.kr/search"
    driver.get(search_url)

    # 키워드 검색
    search_input = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#contents > div.mSearch > form > fieldset > span > input')
            )
        )
    search_input.send_keys(keyword)

    # 검색 필터 및 정렬 기준 선택
    search_button = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "#contents > ul.Retrieval-tab > li:nth-child(4)")
            )
        )
    search_button.click()

    filter_off_button = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div.Cybershop > span.t2 > span.left")
            )
        )
    filter_off_button.click()

    num_pages = 1
    num_posts = 1

    while True:
        logging.info("Get post urls from page %d", num_pages)

        # 현재 페이지의 게시글 목록 가져오기
        post_elms = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#contents > ul.imgList01 > li > a")
                )
            )
        post_urls = [elm.get_attribute('href') for elm in post_elms]
        post_date = [elm.find_element(By.CSS_SELECTOR, 'em.info > span:nth-child(3)').text
                     for elm in post_elms]

        for url, date in zip(post_urls, post_date):
            if str2date(date) < start_date:
                return urls, page_post_list

            urls.append(url)
            page_post_list.append((num_pages, num_posts))
            num_posts += 1

        # 다음 페이지로 이동
        try:
            if num_pages % 5 == 0:
                next_button = driver.find_elements(By.CSS_SELECTOR, "#contents > div > a")[-1]
                if "disabled" in next_button.get_attribute("class"):
                    break
            else:
                next_buttons = driver.find_elements(By.CSS_SELECTOR, "#contents > div > span > a")
                if len(next_buttons) <= (num_pages % 5 - 1):
                    break
                next_button = next_buttons[num_pages % 5 - 1]
        except NoSuchElementException:
            logging.error("Can't find pagination at page %d", num_pages)
            break
        else:
            num_pages += 1
            next_button.click()

    # 브라우저 종료
    return urls


def get_post_info(driver: webdriver.Chrome, wait: WebDriverWait, url: str, 
                  num_pages: int, num_posts: int) -> dict[str, str]:
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
        logging.error('Failed to get post info: page %d, post %d', num_pages, num_posts)
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

    post_data = {
        "title": title,
        "content": content,
        "author": author,
        "created_at": created_at,
        "viewed": viewed,
        "num_of_comments": len(comments),
        "liked": liked,
        "comments": comments
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
        print("Error: Can't find data from the given comment element")
    except StaleElementReferenceException:
        print("Error: The given comment element is stale.")
        return {}

    return {
        "author": author,
        "content": content,
        "created_at": created_at
    }


def lambda_handler(event, context=None):
    """
    lambda_handler
    """
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    keyword = event.get('keyword', None)
    output_file = event.get('output', None)
    resume_post = event.get('resume', 1)
    start_date = event.get('startDate', None)
    s3_url = event.get('s3Url', None)

    # 인자값 검사
    if keyword is None:
        logging.error('Property "keyword" is not set.')
        return {}

    if output_file is None:
        logging.error('Property "output" is not set.')
        return {}

    try:
        resume_post = int(resume_post)
    except ValueError:
        logging.error('Property "resume" should be integer.')
        return {}

    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
    except ValueError:
        logging.error('Property "startDate" should be fit to "%Y-%m-%d %H:%M"')
        return {}

    # 웹드라이버 설정
    driver = initialize_driver()
    # driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 10)

    results = []
    urls, page_post_list = get_post_urls(driver, wait, keyword, start_date)
    for url, (page, post) in zip(urls, page_post_list):
        results.append(get_post_info(driver, wait, url, page, post))

    # 브라우저 종료
    driver.quit()

    return {'results': results}


if __name__ == '__main__':
    response = lambda_handler({
        'keyword': 'iccu',
        'target_prefix': 'bobae',
        'start_date': '2024-01-01 00:00',
        'period': '20',
        'queue_url': ''
    })

    with open('bobae_iccu_test.jsonl', 'w', encoding='utf-8') as f:
        for post in response['results']:
            f.write(json.dumps(post) + '\n')
