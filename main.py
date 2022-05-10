import time
import re

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

target = [
    "https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x487452845de86ef9:0x88a69322ea458c0d!8m2"
    "!3d50.836499!4d-0.777578!16s%2Fg%2F1thcrt88?authuser=0&hl=en&rclk=1"]

float_regex = re.compile(r'\d+(?:\.\d+)?')

output = [["rate", "time", "comment"]]


def page_preload(url):
    driver = webdriver.Chrome(service=Service('tmp/chromedriver'))
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    time.sleep(2)  # Let the user actually see something!
    review_bt = wait.until(ec.element_to_be_clickable((By.XPATH, '//button[@class=\'DkEaL\']')))
    review_bt.click()
    return driver


def get_review_summary(result_set):
    rev_dict = {'Review_Rate': [],
                'Review_Time': [],
                'Review_Text': []}
    for result in result_set:
        review_rate = result.find('span', class_='kvMYJc')["aria-label"]
        review_time = result.find('span', class_='rsqaWe').text
        review_text = result.find('span', class_='wiI7pd').text

        rev_dict['Review_Rate'].append(review_rate)
        rev_dict['Review_Time'].append(review_time)
        rev_dict['Review_Text'].append(review_text)

    return pd.DataFrame(rev_dict)


def page_parser(driver):
    """ parse the page for reviews number and rating"""
    wait = WebDriverWait(driver, 10)
    # n reviews and rating
    first_response = BeautifulSoup(driver.page_source, 'html.parser')
    total_reviews_cnt = int(''.join(filter(str.isdigit, (first_response.find('button', class_='DkEaL')['aria-label']))))
    print(f'Total number of reviews: {total_reviews_cnt}')
    overall_rating = float(float_regex.findall(first_response.find('span', class_='ceNzKf')['aria-label'])[0])
    print(f'Overall rating: {overall_rating} stars')
    # Sort on Newest
    menu_bt = wait.until(ec.element_to_be_clickable((By.XPATH, '//button[@data-value=\'Sort\']')))
    menu_bt.click()
    recent_rating_bt = wait.until(ec.visibility_of_element_located((By.XPATH, "//li[@data-index='1']")))
    recent_rating_bt.click()
    # scroll until you see all the results
    scrollable_div = wait.until(ec.element_to_be_clickable((By.XPATH, '//div[@class=\'m6QErb DxyBCb kA9KIf dS8AEf\']')))
    for i in range(total_reviews_cnt//3):
        print(f"run{i}")
        driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
        time.sleep(0.1)
    second_response = BeautifulSoup(driver.page_source, 'html.parser')
    reviews = second_response.find_all('div', class_='jftiEf L6Bbsd fontBodyMedium')
    return reviews


if __name__ == '__main__':
    for _ in range(1):
        raw_reviews = page_parser(driver=page_preload(target[0]))
        print(get_review_summary(raw_reviews))
