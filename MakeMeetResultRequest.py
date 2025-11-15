from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import json
import asyncio

async def make_meet_results_request(session, token, date, index):
    url = "https://usaswimming.sisense.com/api/datasources/Meets/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps()
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    await asyncio.sleep(0.2*(index%100))
    response = await session.post(url, headers=headers, data=payload, verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome", timeout=40)
    return response.json()
