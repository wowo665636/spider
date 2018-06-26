#!/usr/bin/python
#coding=utf-8
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from pyquery import PyQuery as pq
import json
browser = webdriver.Chrome("/Users/wangdi/Documents/upload/chromedriver")
wait = WebDriverWait(browser, 10)

def index_page(page):


    print('paqu', page, 'ye')
    try:
        url = 'https://s.taobao.com/search?q=iPad'
        browser.get(url)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.m-itemlist .items .item')))
        get_products()
    except TimeoutException:
        index_page(page)


def get_products():
    """
    提取商品数据
    """
    html = browser.page_source
    doc = pq(html)
    items = doc('#mainsrp-itemlist .items .item').items()
    for item in items:
        product = {
            'image': item.find('.pic .img').attr('data-src'),
            'price': item.find('.price').text(),
            'deal': item.find('.deal-cnt').text(),
            'title': item.find('.title').text(),
            'shop': item.find('.shop').text(),
            'location': item.find('.location').text()
        }
        print(json.dumps(product,encoding="UTF-8", ensure_ascii=False))

if __name__ == "__main__":
    index_page(1)