import re, datetime, json
import logging
from urllib import parse
from datetime import timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from tempfile import mkdtemp

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

url='https://section.cafe.naver.com/ca-fe/home/search/articles?{}&p={}&em=1&od=1'
target_extension = 'jsonl'
page_size = 12

# 1. 전체 게시글 중 스크랩 범위의 URL 가져오는 람다
# keyword, target_prefix, start_date, period, bucket={name=~, path=~}  => 날짜만
# 2. 포스트 N개 스크랩()
# args -> urls = [] (for문 처리)


def lambda_handler(event, context):
    keyword = event['keyword']
    target_prefix = event['target_prefix']
    start_number = int(event['start_number'])
    end_number = int(event['end_number']) # 1-50, 51-100 

    start_datetime = datetime.datetime.fromisoformat(event['start_datetime'])
    max_days = int(event['max_days'])
    bucket_name = event['bucket_name']

    current_time = time_now()
    file_name = f"{target_prefix}_{keyword}_{current_time}.{target_extension}"
    file_path = f"/tmp/{file_name}"
    run(keyword, file_path, start_datetime, start_number, end_number, max_days)
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_name)
    return {
        'statusCode': 200,
        'body': json.dumps(f'{current_time} File({keyword}) uploaded successfully!')
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
    

def run(query: str, file_name, start_datetime, start_num: int, end_number: int, max_days: int):
    pattern = r"^\d{4}.\d{2}.\d{2}.$"
    ENCODED_QUERY = parse.urlencode({'q': query})

    driver = get_driver()
    driver.get(url.format(ENCODED_QUERY, 1))
    wait = WebDriverWait(driver, 5)

    total = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#mainContainer > div.content > div.section_home_search > div.search_item_wrap > div.board_head > div.sub_text'))).text
    total = int(total.replace(',', ''))

    start_page = (start_num + page_size - 1) // page_size
    end_page = min((total + page_size - 1) // page_size, 100, end_number // page_size)
    
    post_links = get_page_links(driver, ENCODED_QUERY, pattern, start_datetime, max_days, start_page, end_page)

    get_post_info(driver, post_links, file_name)
    driver.quit()


def get_post_info(driver, post_links, file_name):
    NUM_OF_POSTS = len(post_links)
    for seq, link in enumerate(post_links):
        try:
            driver.get(link)
            wait = WebDriverWait(driver, 5)

            iframe = wait.until(EC.presence_of_element_located((By.ID, "cafe_main")))
            driver.switch_to.frame(iframe)  # iframe으로 전환

            title = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#app > div > div > div.ArticleContentBox > div.article_header > div:nth-child(1) > div > div > h3'))).text
            article_container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'article_container')))
            # content = WebDriverWait(article_container, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'article_viewer'))).text
            content = article_container.find_element(By.CLASS_NAME, 'article_viewer').text
            author = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'profile_info'))).find_element(By.CLASS_NAME, 'nickname').text
            time_block = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'article_info')))
            # created_at = WebDriverWait(time_block, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'date'))).text
            created_at = time_block.find_element(By.CLASS_NAME, 'date').text
            # viewed = WebDriverWait(time_block, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'count'))).text
            viewed = time_block.find_element(By.CLASS_NAME, 'count').text

            article_tool = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'ArticleTool')))
            # num_of_comments = WebDriverWait(article_tool, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'num'))).text
            num_of_comments = article_tool.find_element(By.CLASS_NAME, 'num').text
            like_article = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'like_article')))
            # liked = WebDriverWait(like_article, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'u_cnt._count'))).text
            liked = like_article.find_element(By.CLASS_NAME, 'u_cnt._count').text
            comment_list = WebDriverWait(article_container, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'comment_list')))
            # comment_list = article_container.find_element(By.CLASS_NAME, 'comment_list')
            # comment_blocks = WebDriverWait(comment_list, 5).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'li')))
            comment_blocks = comment_list.find_elements(By.TAG_NAME, 'li')
            comments = get_comment_data(comment_blocks)
            
            post = {
                "title": title,
                "content": content,
            	"author": author,
            	"created_at": created_at,
            	"viewed": viewed,
            	"num_of_comments": num_of_comments,
            	"liked": liked,
            	"comments": comments
            }
            try:
                with open(file_name, "a", encoding='utf8') as target_file:
                    json.dump(post, target_file, ensure_ascii=False)
                    target_file.write('\n')
                logger.info(f"{seq + 1}/{NUM_OF_POSTS}:  {title}")
            except IOError as e:
                logger.error(f"{seq + 1}/{NUM_OF_POSTS}: 파일 쓰기 오류 발생. 제목: {title}, 링크: {link}")
                continue
        except NoSuchElementException as e:
            logger.error(f"{seq + 1}/{NUM_OF_POSTS}: 요소를 찾을 수 없음. 링크: {link}")
        except TimeoutException as e:
            logger.error(f"{seq + 1}/{NUM_OF_POSTS}: 페이지 로딩 시간 초과. 링크: {link}")
        except WebDriverException as e:
            logger.error(f"{seq + 1}/{NUM_OF_POSTS}: 웹 드라이버 오류 발생. 링크: {link}")
            driver.quit()
            driver = get_driver()
        except Exception as e:
            logger.error(f"{seq + 1}/{NUM_OF_POSTS}: 게시글 처리 중 오류 발생. 링크: {link}")


def get_page_links(driver, ENCODED_QUERY, pattern, start_datetime, max_days, start_page, end_page):
    post_links = []
    for page_num in range(start_page, end_page+1):
        driver.get(url.format(ENCODED_QUERY, page_num))
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
                if posted_time < start_datetime - timedelta(days=max_days+1):
                    return post_links
            try:
                article_link = article_item_wrap.find_element(By.TAG_NAME, 'a').get_attribute('href')
            except NoSuchElementException:
                continue
            post_links.append(article_link)
    return post_links


def time_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')


def get_comment_data(comment_blocks):
    comments = []
    comment = None
    children = []
    for comment_block in comment_blocks:
        # comment_box = WebDriverWait(comment_block, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "comment_box")))
        # author = WebDriverWait(comment_box, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'comment_nickname'))).text
        # content = WebDriverWait(comment_box, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'comment_text_view'))).text
        # created_at = WebDriverWait(comment_box, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'comment_info_date'))).text
        comment_box = comment_block.find_element(By.CLASS_NAME, "comment_box")
        author = comment_box.find_element(By.CLASS_NAME, 'comment_nickname').text
        content = comment_box.find_element(By.CLASS_NAME, 'comment_text_view').text
        created_at = comment_box.find_element(By.CLASS_NAME, 'comment_info_date').text
        if 'CommentItem--reply' in comment_block.get_attribute('class'):
            children.append({
                "author": author,
                "content": content,
    		    "created_at":  created_at
            })
            comment["num_of_comments"] += 1
        else:
            if comment:
                comment["children"] = children
                children = []
                comments.append(comment)
            comment = {
                "author": author,
                "content": content,
    		    "created_at":  created_at,
    		    "num_of_comments": 0,
    		    "children": []
            }

    if comment:
        comment["children"] = children
        comments.append(comment)
    return comments
    