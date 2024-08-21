import logging
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd

from tempfile import mkdtemp


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def find_search_pos(wait):
    search_next = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "search_next"))
    )
    search_url = search_next.get_attribute("href")
    search_url_split = search_url.split("&")
    search_pos = (
        int(
            [href.split("=")[1] for href in search_url_split if "search_pos" in href][0]
        )
        - 10000
    )
    return search_url, search_pos


def move_date(wait, driver, date):
    fast_move = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "btn_grey_roundbg.btn_schmove"))
    )
    fast_move.click()
    time.sleep(1)

    input_element = wait.until(EC.presence_of_element_located((By.ID, "calendarInput")))
    driver.execute_script("arguments[0].removeAttribute('readonly');", input_element)
    input_element.clear()
    input_element.send_keys(date)

    button_path = "/html/body/div[2]/div[3]/main/section[1]/article[2]/div[4]/div[3]/div/div[2]/div[2]/button"
    button = wait.until(EC.element_to_be_clickable((By.XPATH, button_path)))
    button.click()
    time.sleep(1)


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

    url = event.get("url")
    keyword = event.get("keyword")
    start_time = event.get("start_date")
    period = event.get("period")
    queue_url = event.get("queue_url")
    prefix = event.get("target_prefix")

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log",
    )
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 10)
        driver.get(url)
        time.sleep(1)
    except Exception:
        logger.error("driver 문제")
        return {"statusCode": 400, "search_url": "", "search_pos": []}

    try:

        search_path = "/html/body/div[2]/div[3]/main/section[1]/article[2]/form[2]/fieldset/div/div[2]/div/input"
        search_box = wait.until(EC.presence_of_element_located((By.XPATH, search_path)))
        search_box.send_keys(keyword)
        time.sleep(1)

        search_button_path = "/html/body/div[2]/div[3]/main/section[1]/article[2]/form[2]/fieldset/div/div[2]/button"
        search_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, search_button_path))
        )
        search_button.click()
        time.sleep(1)
    except:
        logger.error("검색 게시판 찾지 못함")

    search_positions = []
    create_day = start_time.split("T")[0]
    create_date = pd.Timestamp(create_day)
    past_date = create_date - pd.Timedelta(days=period)
    past_date_str = past_date.strftime("%Y-%m-%d")
    logger.info(f"Past Date {past_date_str}")

    try:
        move_date(wait, driver, start_time)
        search_url, current_pos = find_search_pos(wait)

        move_date(wait, driver, past_date_str)
        _, past_pos = find_search_pos(wait)

        while current_pos <= past_pos:
            search_positions.append(current_pos)
            current_pos += 10000

    except:
        logger.error("첫번째 페이지 못찾음")
        search_url = ""

    driver.quit()

    file_name = f"{prefix}_{keyword}_{start_time}.jsonl"

    logger.info(f"search_url {search_url}")
    logger.info(f"search_pos {search_positions}")

    return {
        "statusCode": 200,
        "search_url": search_url,
        "search_pos": search_positions,
        "file_name": file_name,
        "queue_url": queue_url,
    }
