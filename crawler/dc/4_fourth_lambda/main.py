import logging
import os
import time
import re
import boto3
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from tempfile import mkdtemp

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_comments(wait, comment_path):
    try:
        ul_element = wait.until(
            EC.presence_of_element_located((By.XPATH, comment_path))
        )
        li_elements = ul_element.find_elements(By.XPATH, "./li")

        comments = []
        for i in range(len(li_elements)):
            li_element = li_elements[i]
            class_value = li_element.get_attribute("class")

            if class_value == "ub-content":

                try:
                    author = li_element.find_element(
                        By.CSS_SELECTOR, ".cmt_nickbox .nickname em"
                    ).text
                    try:
                        content = li_element.find_element(
                            By.CSS_SELECTOR, ".cmt_txtbox p"
                        ).text
                    except Exception as e:
                        content = " "
                        logger.warn("이모티콘이여서 content " "로 대체 ")

                    created_at = li_element.find_element(
                        By.CSS_SELECTOR, ".fr .date_time"
                    ).text

                    div_element = li_element.find_element(By.TAG_NAME, "div")
                    data_rcnt = div_element.get_attribute("data-rcnt")
                    if data_rcnt is None:
                        data_rcnt = 0
                    comment = {
                        "author": author,
                        "content": content,
                        "created_at": created_at,
                        "num_of_comments": data_rcnt,
                        "children": [],
                    }
                except Exception as e:
                    logger.error("삭제된 댓글")
                    comment = {
                        "author": " ",
                        "content": "해당 댓글은 삭제되었습니다.",
                        "created_at": " ",
                        "num_of_comments": " ",
                        "children": [],
                    }
                if int(data_rcnt) > 0:
                    reply_element = li_elements[i + 1]
                    lis = reply_element.find_elements(By.TAG_NAME, "li")

                    for j in range(len(lis)):
                        li = lis[j]
                        reply_class_value = li_element.get_attribute("class")

                        if reply_class_value == "ub-content":
                            reply_author = li.find_element(
                                By.CSS_SELECTOR, ".cmt_nickbox .nickname em"
                            ).text
                            try:
                                reply_content = li.find_element(
                                    By.CSS_SELECTOR, ".cmt_txtbox p"
                                ).text
                            except Exception as e:
                                reply_content = " "
                                logger.warn("이모티콘이여서 content " "로 대체 ")
                            reply_created_at = li.find_element(
                                By.CSS_SELECTOR, ".fr .date_time"
                            ).text

                            reply = {
                                "author": reply_author,
                                "content": reply_content,
                                "created_at": reply_created_at,
                            }
                            comment["children"].append(reply)
                comments.append(comment)
        return comments
    except Exception:
        logger.error("댓글 오류")
        time.sleep(1)


def get_content(wait, content_path):
    try:
        container = wait.until(EC.presence_of_element_located((By.XPATH, content_path)))
        content = container.find_element(
            By.XPATH, ".//div[@class='write_div']"
        ).text.replace("\n", " ")

        return content

    except Exception:
        logger.error("게시글 내용 오류")
        return None


def get_created_at(wait, created_at_path):
    try:
        container = wait.until(
            EC.presence_of_element_located((By.XPATH, created_at_path))
        )
        spans = container.find_elements(By.XPATH, "./span")

        if len(spans) == 3:
            created_at = spans[2].text
        elif len(spans) < 3:
            created_at = spans[1].text
        else:
            created_at = "Unknown format"

        return created_at

    except Exception:
        logger.error("생성일 오류")
        return None


def lambda_handler(event, context):
    chrome_options = ChromeOptions()
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

    post_url = event.get("post_urls")
    queue_url = event.get("queue_url")

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log",
    )

    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 10)
    except Exception:
        logger.error("driver 문제")
        return {"statusCode": 401}

    try:
        driver.get(post_url)
        wait.until(
            lambda driver: driver.execute_script("return document.readyState")
            == "complete"
        )

        title_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/h3/span[2]"
        title = wait.until(EC.presence_of_element_located((By.XPATH, title_path)))
        title = title.text

        content_path = (
            "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/div/div[1]/div[1]"
        )
        content = get_content(wait, content_path)

        author_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/div/div[1]/span[1]/em"
        author = wait.until(EC.presence_of_element_located((By.XPATH, author_path)))
        author = author.text

        created_at_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/div/div[1]"
        created_at = get_created_at(wait, created_at_path)

        viewed_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/div/div[2]/span[1]"
        viewed = wait.until(EC.presence_of_element_located((By.XPATH, viewed_path)))
        viewed = int(viewed.text.split(" ")[1])

        liked_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/div/div[2]/span[2]"
        liked = wait.until(EC.presence_of_element_located((By.XPATH, liked_path)))
        liked = int(liked.text.split(" ")[1])

        num_of_comments_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[1]/header/div/div/div[2]/span[3]/a"
        num_of_comments = wait.until(
            EC.presence_of_element_located((By.XPATH, num_of_comments_path))
        )
        num_of_comments = int(num_of_comments.text.split(" ")[1])

        if num_of_comments > 0:
            comment_path = "/html/body/div[2]/div[3]/main/section/article[2]/div[3]/div[1]/div[2]/ul"
            comments = extract_comments(wait, comment_path)
        else:
            comments = []
        
        url = driver.current_url

        post_info = {
            "title": title,
            "content": content,
            "author": author,
            "created_at": created_at,
            "viewed": viewed,
            "liked": liked,
            "num_of_comments": num_of_comments,
            "comments": comments,
            "url": url,
        }

    except Exception as e:
        logger.error("마지막 값 가져오기 오류")
        driver.quit()
        return {"statusCode": 402}

    driver.quit()

    # 데이터를 SQS 큐에 전송
    sqs = boto3.client("sqs")
    response = sqs.send_message(
        QueueUrl=queue_url, MessageBody=json.dumps(post_info, ensure_ascii=False)
    )

    return {}
