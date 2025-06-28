from curl_cffi import requests
import json

def make_meet_request(token, startdate, enddate):
    url = "https://usaswimming.sisense.com/api/datasources/Meets/jaql?trc=sdk-ui-1.23.0"
    
    payload = json.dumps()

    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload, 
                                verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome").json()
    return response