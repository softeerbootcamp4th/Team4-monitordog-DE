import logging
from datetime import datetime, timedelta
from tempfile import mkdtemp

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
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
                  keyword: str, start_datetime: datetime, period) -> list[str]:
    """
    keyword 검색 결과에 해당하는 posts의 URL을 가져온다.
    :param driver: Selenium의 WebDriver, initialize_driver를 통해 생성된 인스턴스를 집어넣습니다.
    :param wait: driver를 통해 만들어진 Wait 인스턴스
    :param keyword: 검색 키워드
    :param start_date: 가져올 게시글의 시작 날짜
    """
    start_date = start_datetime.date()

    urls = []

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
            if str2date(date) < start_date - timedelta(days=period+1):
                return urls
            elif str2date(date) > start_date:
                continue
            urls.append(url)
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


def lambda_handler(event, context=None):
    """
    lambda_handler
    """
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    keyword = event.get('keyword', None)
    target_prefix = event.get('target_prefix', None)
    period = event.get('period', 1)
    start_date = event.get('start_date', None)
    queue_url = event.get('queue_url', None)

    # 인자값 검사
    if keyword is None:
        logging.error('Property "keyword" is not set.')
        return {}

    if target_prefix is None:
        logging.error('Property "output" is not set.')
        return {}

    try:
        period = int(period)
    except ValueError:
        logging.error('Property "resume" should be integer.')
        return {}

    try:
        start_date = datetime.fromisoformat(start_date)
    except ValueError:
        logging.error('Property "startDate" should be fit to "%Y-%m-%d %H:%M"')
        return {}

    # 웹드라이버 설정
    driver = initialize_driver()
    wait = WebDriverWait(driver, 10)

    urls = get_post_urls(driver, wait, keyword, start_date, period)

    # 브라우저 종료
    driver.quit()

    return {
        'statusCode': 200,
        'body': {
            'links': [[url] for url in urls],
            'file_name': f"{target_prefix}_{keyword}_{event['start_date']}",
            'queue_url': queue_url
        }
    }
