from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import json
import asyncio

async def make_rankings_results_request(session, token, ids, index):
    url = "https://usaswimming.sisense.com/api/datasources/USA%20Swimming%20Times%20Elasticube/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps()

    print(f"request{index/150}")
    response = await session.post(url, headers=headers, data=payload, verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome", timeout=40)
    return response.json()