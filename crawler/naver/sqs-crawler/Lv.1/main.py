import re
import datetime
from urllib import parse
from datetime import timedelta
from pytz import timezone

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from tempfile import mkdtemp


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

URL='https://section.cafe.naver.com/ca-fe/home/search/articles?{}&p={}&em=1&od=1'
TARGET_FORMAT = 'jsonl'
MAX_READABLE_PAGE = 100 # 네이버 카페 통합검색은 100페이지까지

# 1. 전체 게시글 중 스크랩 범위의 URL 가져오는 람다
# keyword, target_prefix, start_date, period, bucket={name=~, path=~}  => 날짜만
# 2. 포스트 N개 스크랩()
# args -> urls = [] (for문 처리)


def lambda_handler(event, context):
    keyword = event['keyword']
    target_prefix = event['target_prefix']
    start_date = datetime.datetime.fromisoformat(event['start_date'])
    period = int(event['period'])
    queue_url = event['queue_url']

    current_time = time_now()
    file_name = f"{target_prefix}_{keyword}_{event['start_date']}.{TARGET_FORMAT}"

    post_links = get_page_links(keyword, current_time, start_date, period)

    body = {
        "file_name": file_name,
        "queue_url": queue_url,
        "links": [[link] for link in post_links]
    }    

    return {
        "statusCode": 200, 
        "body": body # json.dumps(body, ensure_ascii=False)#, indent=4)
    }


def get_driver():
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

    return webdriver.Chrome(
        service=service,
        options=chrome_options
    )
    

def get_page_links(query: str, current_time, start_date, period: int):
    pattern = r"^\d{4}.\d{2}.\d{2}.$"
    ENCODED_QUERY = parse.urlencode({'q': query})

    driver = get_driver()
    driver.get(URL.format(ENCODED_QUERY, 1))
    wait = WebDriverWait(driver, 5)

    post_links = []
    for page_num in range(1, MAX_READABLE_PAGE+1):
        driver.get(URL.format(ENCODED_QUERY, page_num))
        wait = WebDriverWait(driver, 5)
        item_list = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'item_list')))
        try:
            articleItems = item_list.find_elements(By.CLASS_NAME, 'ArticleItem')
        except NoSuchElementException:
            continue
        for article in articleItems:
            try:
                article_item_wrap = article.find_element(By.CLASS_NAME, "article_item_wrap")
                posted_time = article_item_wrap.find_element(By.CLASS_NAME, 'date').text
            except (NoSuchElementException, StaleElementReferenceException):
                continue
            if bool(re.match(pattern, posted_time)):
                posted_time = datetime.datetime.strptime(posted_time, "%Y.%m.%d.")
                
                if posted_time < start_date - timedelta(days=period+1):
                    return post_links
                elif posted_time > start_date:
                    continue
            else: 
                if current_time > start_date.astimezone(timezone('Asia/Seoul')):
                    continue
            try:
                article_link = article_item_wrap.find_element(By.TAG_NAME, 'a').get_attribute('href')
            except NoSuchElementException:
                continue
            post_links.append(article_link)
    driver.quit()
    return post_links


def time_now():
    return datetime.datetime.now(timezone('Asia/Seoul'))

    