import requests
from bs4 import BeautifulSoup

def get_proxies(url):
    # Send a request to the proxy list website
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table containing the proxy list
    table = soup.select("#list > div > div.table-responsive > div > table > tbody > tr")
    proxies = []

    # Iterate through each row in the table
    for i in range(len(table)):
        elem = table[i]
        ip = elem.select_one("td:nth-child(1)").get_text(strip=True)
        port = elem.select_one("td:nth-child(2)").get_text(strip=True)
        code = elem.select_one("td:nth-child(3)").get_text(strip=True)
        https = elem.select_one("td.hx").get_text(strip=True)

        if code == 'US' and https == 'yes':
            proxies.append({"http": f"http://{ip}:{port}", "https": f"https://{ip}:{port}"})

        if len(proxies) == 5:
            break

    return proxies
