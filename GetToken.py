from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

def get_token():
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument("--headless=new")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get("https://data.usaswimming.org/datahub/usas/individualsearch")
    WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.ID, "firstOrPreferredName")))
    browser_log = driver.get_log('performance') 
    for event in browser_log:
        event = str(event)
        if "Bearer" in event:
            authorization_index = event.index("Authorization")
            bearer_index = event.index("Bearer")
            if bearer_index - authorization_index <= 17:
                bearer_token = event[bearer_index+7:bearer_index + event[bearer_index:].index(",")-1]
                if "undefined" not in bearer_token:
                    break
    return bearer_token

if __name__ == "__main__":
    print(get_token())