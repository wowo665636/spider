#!/usr/bin/python
#coding=utf-8
import time
import unittest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

class PythonOrgSearch(unittest.TestCase):

    def setUp(self):
        self.driver = webdriver.Chrome("/Users/wangdi/Documents/upload/chromedriver")

    def test_search_in_python_org(self):
        driver = self.driver
        driver.get("https://www.instagram.com/lionelferro/")
        time.sleep(10)
        #self.assertIn("Python", driver.title)
        # elem = driver.find_element_by_id("submit")
        # elem.send_keys("Event – PyCon PL 2015")
        # elem.send_keys(Keys.RETURN)
        link_a = driver.find_element_by_tag_name('h1')
        print(link_a)

    def tearDown(self):
        self.driver.close()

if __name__ == "__main__":
    unittest.main()
