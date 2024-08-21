import logging
import os
import time
import re
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


def find_last_page(element):
    try:
        sub_element = element.find_element(By.CLASS_NAME, "sp_pagingicon.page_end")
        href_value = sub_element.get_attribute("href").split("&page=")[1]
        last_page = int(href_value.split("&")[0])
        return last_page
    except Exception:
        pass

    a_tags = element.find_elements(By.TAG_NAME, "a")
    a_tage_last = [link for link in a_tags if not link.get_attribute("class")]
    if len(a_tage_last) == 0:
        return 1
    else:
        return int(a_tage_last[-1].text)


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

    url = event.get("search_url")
    pos = event.get("search_pos")
    queue_url = event.get("queue_url")

    split_url = re.split(r"&search_pos=.*?&", url)
    modified_url = split_url[0] + "&search_pos=" + str(pos) + "&" + split_url[1]

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log",
    )
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 10)
    except Exception:
        logger.error("driver 문제")
        return {"statusCode": 400, "page_urls": []}

    page_urls = []
    try:
        driver.get(modified_url)
        time.sleep(1)

        element = wait.until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "bottom_paging_box.iconpaging")
            )
        )
        last_page = find_last_page(element)

        for i in range(1, last_page + 1):
            split_url = re.split(r"&page=.*?&", modified_url)
            url = split_url[0] + "&page=" + str(i) + "&" + split_url[1]
            page_urls.append(url)

    except Exception as e:
        last_page = 0
        logger.error("페이지 수 못찾음")

    driver.quit()

    return {
        "statusCode": 200,
        "page_urls": page_urls,
        "queue_url": queue_url,
    }
