from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import json
import asyncio

async def make_meet_keys_request(session, token, dates):
    url = "https://usaswimming.sisense.com/api/datasources/Meets/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps({
        "metadata":[
            {"jaql":{"title":"Meet Name", "dim":"[Meet.MeetName]", "datatype":"text"}},
            {"jaql":{"title":"Meet Type", "dim":"[Meet.MeetType]", "datatype":"text"}},
            {"jaql":{"title":"Host LSC", "dim":"[OrgUnit.Level3Code]", "datatype":"text"}},
            {"jaql":{"title":"Host Team", "dim":"[OrgUnit.Level4Name]", "datatype":"text"}},
            {"jaql":{
                "title":"Start Date",
                "dim":"[Meet.StartDate (Calendar)]",
                "datatype":"datetime", 
                "level":"days", "sort":"asc", "filter":{"members":dates}}, "format":{"mask":{"days":"M/d/yyyy"}}},
            {"jaql":{"title":"End Date", "dim":"[Meet.EndDate (Calendar)]", "datatype":"datetime", "level":"days"}, "format":{"mask":{"days":"M/d/yyyy"}}},
            {"jaql":{"title":"MeetKey", "dim":"[Meet.MeetKey]", "datatype":"numeric"}},
            {"jaql":{"title":"MeetResultsUrl", "dim":"[Meet.MeetResultsUrl]", "datatype":"text"}},
            {"jaql":{"title":"MeetName", "dim":"[Meet.MeetName]", "datatype":"text"}, "panel":"scope"},
            {"jaql":{"title":"MeetType", "dim":"[Meet.MeetType]", "datatype":"text"}, "panel":"scope"}],
            "datasource":{"title":"Meets","live":False},
            "by":"ComposeSDK","queryGuid":"51ddbb96-ebaf-4f70-a22e-c0ed070db758"})

    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    response = await session.post(url, headers=headers, data=payload, verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome", timeout=40)
    return response.json()
