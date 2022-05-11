import datetime
import time
import re
from multiprocessing import Process

import pandas as pd
import selenium.common.exceptions
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

float_regex = re.compile(r'\d+(?:\.\d+)?')

stores = {
    'BLUEWATER': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.4384654,0.2744905,17z/data=!3m1!4b1!4m6!3m5!1s0x47d8b14fc5d6b913:0x39926dcb761dc966!8m2!3d51.4384654!4d0.2744905!16s%2Fg%2F11bzyx3mf8?authuser=0&hl=en',
    'KINGSTON': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.4107238,-0.3058677,17z/data=!3m1!4b1!4m5!3m4!1s0x48760beabb2f481b:0x7dfd2f3412db0fc3!8m2!3d51.4107205!4d-0.303679?authuser=0&hl=en',
    'MEADOWHALL': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.4145018,-1.4131213,17z/data=!3m1!4b1!4m5!3m4!1s0x48799c3fda4a1625:0x9045739cd380bf34!8m2!3d53.4144986!4d-1.4109326?authuser=0&hl=en',
    'NEWCASTLE': 'https://www.google.com/maps/place/Marks+and+Spencer/@54.9760962,-1.6157552,17z/data=!3m2!4b1!5s0x487e70ca3777a5ed:0x7edf0a3ab3fd43e0!4m5!3m4!1s0x487e70c97a71f7fb:0x432de5aaf78a2785!8m2!3d54.9760931!4d-1.6135665?authuser=0&hl=en',
    'STRATFORD_CITY_LONDON': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5423883,-0.0071821,17z/data=!3m2!4b1!5s0x48761d63ede56bff:0xde73a0c5581d7615!4m5!3m4!1s0x48761d6e4ae43e43:0x6b59a161420277e2!8m2!3d51.542385!4d-0.0049934?authuser=0&hl=en',
    'MANCHESTER': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.483467,-2.244074,17z/data=!3m1!4b1!4m6!3m5!1s0x487bb1c6b7596c07:0x46f76ea980e86713!8m2!3d53.483467!4d-2.244074!16s%2Fg%2F1tf0q4y5?authuser=0&hl=en',
    'NORWICH': 'https://www.google.com/maps/place/Marks+and+Spencer/@52.6263948,1.2928239,17z/data=!3m1!4b1!4m6!3m5!1s0x47d9e3e41c4b219f:0xe00fad8f95a8324d!8m2!3d52.6263948!4d1.2928239!16s%2Fg%2F1tr16vl5?authuser=0&hl=en',
    'PLYMOUTH': 'https://www.google.com/maps/place/Marks+and+Spencer/@50.3721678,-4.1398409,17z/data=!3m1!4b1!4m6!3m5!1s0x486c934c99431e01:0x22d025943fcfbf4d!8m2!3d50.3721678!4d-4.1398409!16s%2Fg%2F1tc_dpdw?authuser=0&hl=en',
    'CRIBBS_CAUSEWAY': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5238332,-2.5941132,17z/data=!3m1!4b1!4m6!3m5!1s0x487191805ee6fc2f:0xef1b4cac1346a8ff!8m2!3d51.5238332!4d-2.5941132!16s%2Fg%2F1tpprdqj?authuser=0&hl=en',
    'LISBURN': 'https://www.google.com/maps/place/Marks+and+Spencer/@54.490559,-6.0577629,17z/data=!3m1!4b1!4m6!3m5!1s0x4861049ab3628723:0xaa8b226f191e5b84!8m2!3d54.490559!4d-6.0577629!16s%2Fg%2F1thl4vcq?authuser=0&hl=en',
    'LIFFEY_VALLEY_DUBLIN': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.3539718,-6.3907645,17z/data=!3m2!4b1!5s0x486772d6bdb93e75:0x7edf0a3a1f8b225c!4m5!3m4!1s0x4867734f13cea27f:0x7ff054cafefa61c1!8m2!3d53.3539686!4d-6.3885758?authuser=0&hl=en',
    'THURROCK': 'https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x47d8b7333434f021:0x10aa4eda696ed908!8m2!3d51.4874806!4d0.282541!16s%2Fg%2F1tmbvcs9?authuser=0&hl=en&rclk=1',
    'EALING_BROADWAY': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5129633,-0.3060547,17z/data=!3m2!4b1!5s0x48760df508d2f197:0x7edf0a3a7fc64bcc!4m5!3m4!1s0x48760df56ca84d33:0xdee9d5f11a9f6076!8m2!3d51.51296!4d-0.303866?authuser=0&hl=en',
    'MERRYHILL': 'https://www.google.com/maps/place/Marks+and+Spencer/@52.4818744,-2.115021,17z/data=!3m2!4b1!5s0x487090e87d356aa1:0x7edf0a3ae2c50e5c!4m5!3m4!1s0x487090e656990a65:0x7423ea35ede3b944!8m2!3d52.4818712!4d-2.1128323?authuser=0&hl=en',
    'PANTHEON': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.515472,-0.138323,17z/data=!3m1!4b1!4m6!3m5!1s0x48761b2ad867c077:0x1a6d729784c327a0!8m2!3d51.515472!4d-0.138323!16s%2Fg%2F1tc_3k_f?authuser=0&hl=en',
    'MARY_ST_DUBLIN': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5140685,-0.1540799,17z/data=!3m1!4b1!4m6!3m5!1s0x48760532d24c4527:0xa142893b8e675419!8m2!3d51.5140685!4d-0.1540799!16s%2Fg%2F1td3l1v5?authuser=0&hl=en',
    'HANDFORTH': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.3489652,-2.2081197,17z/data=!3m1!4b1!4m5!3m4!1s0x487a4d21d90c3c4b:0x82a4b58bc10236da!8m2!3d53.348962!4d-2.205931?authuser=0&hl=en',
    'BRAEHEAD': 'https://www.google.com/maps/place/M%26S+Simply+Food/@55.8758242,-4.3648586,17z/data=!3m1!4b1!4m6!3m5!1s0x48884f43fcde64bf:0xb60fae35ac3ab229!8m2!3d55.8758242!4d-4.3648586!16s%2Fg%2F1tmz6j45?authuser=0&hl=en',
    'MARBLE_ARCH': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5140718,-0.1562686,17z/data=!3m1!4b1!4m5!3m4!1s0x48760532d24c4527:0xa142893b8e675419!8m2!3d51.5140685!4d-0.1540799?authuser=0&hl=en',
    'MILTON_KEYNES': 'https://www.google.com/maps/place/Marks+and+Spencer/@52.0416863,-0.7612897,17z/data=!3m1!4b1!4m5!3m4!1s0x4877aa98270e9eed:0x17d65b490b31046!8m2!3d52.041683!4d-0.759101?authuser=0&hl=en',
    'YORK': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.9587491,-1.0801449,17z/data=!3m1!4b1!4m6!3m5!1s0x487931aedf553ecf:0x4215d7705f55e484!8m2!3d53.9587491!4d-1.0801449!16s%2Fg%2F1tdvp85d?authuser=0&hl=en',
    'WATFORD': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.654954,-0.3973246,17z/data=!3m2!4b1!5s0x48766ac4374a403b:0x7edf0a3aeb83346c!4m5!3m4!1s0x48766ac46b1cda59:0x79b4c2cf8df9f869!8m2!3d51.6549507!4d-0.3951359?authuser=0&hl=en',
    'BATH': 'https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x4871811173d66a49:0xaee36851efb594b6!8m2!3d51.3798192!4d-2.3591237!16s%2Fg%2F1tfpp465?authuser=0&hl=en&rclk=1',
    'BRENT_CROSS': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.5760263,-0.2252421,17z/data=!3m2!4b1!5s0x487610e8742540c7:0x277c836c42d9393!4m5!3m4!1s0x487610e7db6aac5f:0x8cb11eab10a36659!8m2!3d51.576023!4d-0.2230534?authuser=0&hl=en',
    'CAMBRIDGE': 'https://www.google.com/maps/place/Marks+and+Spencer/@52.205863,0.1212126,17z/data=!3m1!4b1!4m6!3m5!1s0x47d8709618838619:0x40ac0b7a34ab1c50!8m2!3d52.205863!4d0.1212126!16s%2Fg%2F1yglpdg_d?authuser=0&hl=en',
    'HEDGE_END': 'https://www.google.com/maps/place/Marks+and+Spencer/@50.9205545,-1.3133229,17z/data=!3m1!4b1!4m6!3m5!1s0x48747202363b800f:0x7fa5a6223e468da0!8m2!3d50.9205545!4d-1.3133229!16s%2Fg%2F1th1th_8?authuser=0&hl=en',
    'DOUGLAS': 'https://www.google.com/maps/place/Marks+and+Spencer/@54.1502328,-4.481845,17z/data=!3m1!4b1!4m5!3m4!1s0x48638517229c80eb:0xb8ab9d15e8f5bafb!8m2!3d54.1502297!4d-4.4796563?authuser=0&hl=en',
    'LEEDS': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.7969664,-1.5449747,17z/data=!3m2!4b1!5s0x48795c194dd4524b:0x8202018bb83e8fc5!4m5!3m4!1s0x48795c19107df333:0x439a911532d840e0!8m2!3d53.7969633!4d-1.542786?authuser=0&hl=en',
    'CARDIFF': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.4812556,-3.174086,17z/data=!3m1!4b1!4m6!3m5!1s0x486e1cb73fc7a749:0x14fd0cd4061f8225!8m2!3d51.4812556!4d-3.174086!16s%2Fg%2F1v4k5yw7?authuser=0&hl=en',
    'LIVERPOOL': 'https://www.google.com/maps/place/Marks+and+Spencer/@53.405394,-2.983592,17z/data=!3m1!4b1!4m6!3m5!1s0x487b21256f5ad1bf:0x72feb069fe4bda6b!8m2!3d53.405394!4d-2.983592!16s%2Fg%2F1xgrmgvn?authuser=0&hl=en',
    'EDINBURGH': 'https://www.google.com/maps/place/Marks+and+Spencer/@55.9526296,-3.1946749,17z/data=!3m1!4b1!4m6!3m5!1s0x4887c7902a0eae7b:0x4b30cfc35cd373bd!8m2!3d55.9526296!4d-3.1946749!16s%2Fg%2F1tm8k4p4?authuser=0&hl=en',
    'CARLISLE': 'https://www.google.com/maps/place/Marks+and+Spencer/@54.893507,-2.935521,17z/data=!3m1!4b1!4m6!3m5!1s0x487d1a260f5e6e91:0x53bb204409ab044e!8m2!3d54.893507!4d-2.935521!16s%2Fg%2F11bzy_4tc0?authuser=0&hl=en',
    'CROYDON': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.377562,-0.101988,17z/data=!3m1!4b1!4m6!3m5!1s0x48760731b173997b:0x6ffa53fd53c22bc1!8m2!3d51.377562!4d-0.101988!16s%2Fg%2F1tglyg7g?authuser=0&hl=en',
    'CAMBERLEY': 'https://www.google.com/maps/place/M%26S+Bureau+De+Change+Camberley/@51.3356656,-0.7773664,17z/data=!3m1!4b1!4m5!3m4!1s0x48742a98792a423f:0xc1b6dd8b15a78887!8m2!3d51.3356623!4d-0.7751777?authuser=0&hl=en',
    'DERBY_WESTFIELD': 'https://www.google.com/maps/place/Marks+and+Spencer/@52.9183139,-1.4741464,17z/data=!3m2!4b1!5s0x4879f1181db2996d:0x7edf0a3adc6871ec!4m5!3m4!1s0x4879f1175db9f4db:0x62dde9b44f40be66!8m2!3d52.9183107!4d-1.4719577?authuser=0&hl=en',
    'CHICHESTER': 'https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x487452845de86ef9:0x88a69322ea458c0d!8m2!3d50.836499!4d-0.777578!16s%2Fg%2F1thcrt88?authuser=0&hl=en&rclk=1',
    'TUNBRIDGE_WELLS': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.1342345,0.2657191,17z/data=!3m1!4b1!4m6!3m5!1s0x47df4432714f4db7:0xda1ba4741cee8982!8m2!3d51.1342345!4d0.2657191!16s%2Fg%2F1tdxpr1h?authuser=0&hl=en',
    'CHELMSFORD': 'https://www.google.com/maps/place/M%26S+Bureau+De+Change+Chelmsford/@51.7322558,0.4716118,17z/data=!3m1!4b1!4m5!3m4!1s0x47d8ebd635dfb3ff:0x50eea4860ddeb6ce!8m2!3d51.7321514!4d0.4738882?authuser=0&hl=en',
    'ST_ALBANS': 'https://www.google.com/maps/place/Marks+and+Spencer/@51.7531634,-0.3387422,17z/data=!3m1!4b1!4m6!3m5!1s0x48763f24214826a7:0x58d496bef3a42f83!8m2!3d51.7531634!4d-0.3387422!16s%2Fg%2F1ptxnd3r8?authuser=0&hl=en',
    'CHELTENHAM': 'https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x48711b97030657c7:0x45986bbd2e40e1b9!8m2!3d51.9006461!4d-2.0737432!16s%2Fg%2F1tp_5v90?authuser=0&hl=en&rclk=1',
    'ABERDEEN': 'https://www.google.com/maps/place/Marks+and+Spencer/data=!4m6!3m5!1s0x488411ea545356cd:0x4b21a0ace1c0087d!8m2!3d57.1548009!4d-2.1377847!16s%2Fg%2F11g0yk6wrb?authuser=0&hl=en&rclk=1'
}


def page_preload(driver, url):
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


def page_parser(driver, name):
    """ parse the page for reviews number and rating"""
    wait = WebDriverWait(driver, 20)
    # n reviews and rating
    first_response = BeautifulSoup(driver.page_source, 'html.parser')
    total_reviews_cnt = int(''.join(filter(str.isdigit, (first_response.find('button', class_='DkEaL')['aria-label']))))
    print(f)
    overall_rating = float(float_regex.findall(first_response.find('span', class_='ceNzKf')['aria-label'])[0])
    print(f'Store Name: {name}, Total number of reviews: {total_reviews_cnt},Overall rating: {overall_rating} stars')
    # Sort on Newest
    menu_bt = wait.until(ec.element_to_be_clickable((By.XPATH, '//button[@data-value=\'Sort\']')))
    menu_bt.click()
    recent_rating_bt = wait.until(ec.visibility_of_element_located((By.XPATH, "//li[@data-index='1']")))
    recent_rating_bt.click()
    # scroll until you see all the results
    scrollable_div = wait.until(ec.element_to_be_clickable((By.XPATH, '//div[@class=\'m6QErb DxyBCb kA9KIf dS8AEf\']')))

    reviews = []
    prev = -1
    while len(reviews) != total_reviews_cnt:
        if len(reviews) == prev:
            break
        prev = len(reviews)
        for i in range(total_reviews_cnt // 10):
            # searching for clicks
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(0.1)
            buttons = driver.find_elements(by=By.XPATH, value="//button[@class=\'w8nwRe kyuRq\']")
            for button in buttons:
                button.click()
            time.sleep(0.1)
        html_response = BeautifulSoup(driver.page_source, 'html.parser')
        reviews = html_response.find_all('div', class_='jftiEf L6Bbsd fontBodyMedium')
    return reviews


def f(name, url):
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('--headless')
    # chrome_options.add_argument('start-maximized')
    driver = webdriver.Chrome(service=Service('tmp/chromedriver'), options=chrome_options)

    raw_reviews = page_parser(driver=page_preload(driver, url=url), name=name)
    get_review_summary(raw_reviews).to_csv(f'output/{name}.csv')


if __name__ == '__main__':
    start = datetime.datetime.now()
    p = [Process(target=f, args=(k, v)) for k, v in stores.items()]
    l_stores = len(stores)
    print(f"Total Numer of stores {l_stores}")
    for x in range(0, l_stores, 5):
        for j in range(x, min(l_stores, x + 5)): p[j].start()
        for j in range(x, min(l_stores, x + 5)): p[j].join()
    print("Total Time:", datetime.datetime.now() - start)
