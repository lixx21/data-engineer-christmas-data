from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import os

print(os.listdir())


chromeOptions = Options()
chromeOptions.add_argument("--headless")  # Run headless for better performance
chromeOptions.add_argument("--no-sandbox")
chromeOptions.add_argument("--disable-dev-shm-usage")

# chromedriver_path = r"/chromedriver/chromedriver.exe"  # Update with your path
# service = Service(chromedriver_path)
# driver = webdriver.Chrome(service=service, options=chromeOptions)

driver = webdriver.Remote(
   command_executor="http://selenium-hub:4444/wd/hub",
    options=chromeOptions
    )

driver.implicitly_wait(2)
time.sleep(3)

total_page = 5
page = 2

driver.get(f'https://www.walmart.com/shop/savings')

# product_name = driver.find_elements("xpath", '//span[@class="normal dark-gray mb0 mt1 lh-title f6 f5-l lh-copy"]')
# price = driver.find_elements("xpath", '//div[@class="flex flex-wrap justify-start items-center lh-title mb1"] //span[@class="w_iUH7"]')
product_name = driver.find_elements("xpath",'//span[@class="normal dark-gray mb0 mt1 lh-title f6 f5-l"]')

print(f"total products scrapped: {len(product_name)}")
# print(f"total price scrapped: {len(price)}")

for i in range (len(product_name)):
    print(product_name[i].text)

driver.quit()