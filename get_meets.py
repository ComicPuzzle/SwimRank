#from selenium import webdriver
import json
from MakeMeetRequest import make_meet_request
from GetProxies import get_proxies
from GetToken import get_token
from seleniumwire import webdriver
from fp.fp import FreeProxy
import pandas as pd

#Once complete only do one in a final overall script
options = webdriver.ChromeOptions()
options.page_load_strategy = 'eager'
options.add_argument("--headless=new")
options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
driver = webdriver.Chrome(options=options)
driver.get("https://data.usaswimming.org/datahub/usas/individualsearch")
bearer_token = get_token(driver)
print(bearer_token)

offset = 0
row_list = []
length_of_response = 10000
count = 0
with open("meets.json", "w") as file:
    pass

headers = []
df = pd.DataFrame(row_list, columns=headers)
print(df.head(5))
df.to_json('ids.json', orient='records', lines=True)




