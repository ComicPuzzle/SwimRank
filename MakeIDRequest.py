from curl_cffi import requests
import json

def make_id_request(token, count, offset):
    url = "https://usaswimming.sisense.com/api/datasources/Public%20Person%20Search/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps({
    "metadata": [
        {
        "jaql": {
            "title": "Name",
            "dim": "[Persons.FullName]",
            "datatype": "text"
        }
        },
        {
        "jaql": {
            "title": "Club",
            "dim": "[Persons.ClubName]",
            "datatype": "text",
            "sort": "asc"
        }
        },
        {
        "jaql": {
            "title": "LSC",
            "dim": "[Persons.LscCode]",
            "datatype": "text"
        }
        },
        {
        "jaql": {
            "title": "Age",
            "dim": "[Persons.Age]",
            "datatype": "numeric"
        }
        },
        {
        "jaql": {
            "title": "PersonKey",
            "dim": "[Persons.PersonKey]",
            "datatype": "numeric"
        }
        },
        {
        "jaql": {
            "title": "FirstAndPreferredName",
            "dim": "[Persons.FirstAndPreferredName]",
            "datatype": "text",
            "filter": {
            "contains": f""
            }
        },
        "panel": "scope"
        },
        {
        "jaql": {
            "title": "LastName",
            "dim": "[Persons.LastName]",
            "datatype": "text",
        },
        "panel": "scope"
        }
    ],
    "datasource": {
        "title": "Public Person Search",
        "live": False
    },
    "by": "ComposeSDK",
    "queryGuid": "0927011a-07e3-4f71-ab6e-b955c5d0eb1c",
    "count": count,
    "offset": offset,
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload, 
                                verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome").json()
    return response