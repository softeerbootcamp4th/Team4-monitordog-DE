import json
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
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


# sqs 클라이언트 생성
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    post_links = event['links']
    file_name = event['file_name']
    file_path = f"/tmp/{file_name}"
    queue_url = event['queue_url']

    driver = get_driver()

    get_post_info(driver, post_links, file_path)

    try:
        messages = ""
        # 파일 읽기
        with open(file_path, 'r') as file:
            for line in file:
                # 각 라인(메시지)을 읽어서 SQS로 전송
                json_line = line 
                messages += f"{json_line}\n"
                
        # SQS로 메시지 전송
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=messages
        )
        logger.info(f"Message sent to SQS: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Error occurred while sending message to SQS: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"There are No Message.")
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Messages sent to SQS successfully')
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
            content = article_container.find_element(By.CLASS_NAME, 'article_viewer').text
            author = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'profile_info'))).find_element(By.CLASS_NAME, 'nickname').text
            time_block = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'article_info')))
            created_at = time_block.find_element(By.CLASS_NAME, 'date').text
            viewed = time_block.find_element(By.CLASS_NAME, 'count').text

            article_tool = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'ArticleTool')))
            num_of_comments = article_tool.find_element(By.CLASS_NAME, 'num').text
            like_article = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'like_article')))
            liked = like_article.find_element(By.CLASS_NAME, 'u_cnt._count').text
            comment_list = WebDriverWait(article_container, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'comment_list')))
            comment_blocks = comment_list.find_elements(By.TAG_NAME, 'li')
            comments = get_comment_data(comment_blocks)
            url = driver.current_url
            
            post = {
                "title": title,
                "content": content,
            	"author": author,
            	"created_at": created_at,
            	"viewed": viewed,
            	"num_of_comments": num_of_comments,
            	"liked": liked,
            	"comments": comments,
                "url": url
            }
            try:
                with open(file_name, "a", encoding='utf8') as target_file:
                    json_line = json.dumps(post, ensure_ascii=False)
                    target_file.write(f'{json_line}\n')
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


def get_comment_data(comment_blocks):
    comments = []
    comment = None
    children = []
    for comment_block in comment_blocks:
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
    