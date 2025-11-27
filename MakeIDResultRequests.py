from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import json
import asyncio
import time

async def make_id_results_request(session, token, ids, index):
    url = "https://usaswimming.sisense.com/api/datasources/USA%20Swimming%20Times%20Elasticube/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps({"metadata":[
            {"jaql":{"title":"Name","dim":"[UsasSwimTime.FullName]","datatype":"text"}},
            {"jaql":{"title":"Gender","dim":"[EventCompetitionCategory.TypeName]","datatype":"text"}},
            {"jaql":{"title":"Age","dim":"[UsasSwimTime.AgeAtMeetKey]","datatype":"numeric"}},
            {"jaql":{"title":"AgeGroup","dim":"[Age.AgeGroup1]","datatype":"text"}},
            {"jaql":{"title":"Event","dim":"[SwimEvent.EventCode]","datatype":"text"}},
            {"jaql":{"title":"Place","dim":"[UsasSwimTime.FinishPosition]","datatype":"numeric","sort":"asc"}},
            {"jaql":{"title":"Session","dim":"[Session.SessionName]","datatype":"text"}},
            {"jaql":{"title":"Points","dim":"[UsasSwimTime.PowerPoints]","datatype":"numeric"}},
            {"jaql":{"title":"Swim Date", "dim":"[SeasonCalendar.CalendarDate (Calendar)]", "datatype":"datetime","level":"days", "filter":{"from":"2016-1-1"}}, "format":{"mask":{"days":"MM/dd/yyyy"}}},
            {"jaql":{"title":"LSC","dim":"[OrgUnit.Level3Code]","datatype":"text"}},
            {"jaql":{"title":"Team","dim":"[OrgUnit.Level4Name]","datatype":"text"}},
            {"jaql":{"title":"Meet","dim":"[Meet.MeetName]","datatype":"text"}}, 
            {"jaql":{"title":"SwimTime","dim":"[UsasSwimTime.SwimTimeFormatted]","datatype":"text"}},
            {"jaql":{"title":"TimeStandard","dim":"[TimeStandard.TimeStandardName]","datatype":"text"}},
            {"jaql":{"title":"MeetKey","dim":"[Meet.MeetKey]","datatype":"numeric"}},
            {"jaql":{"title":"UsasSwimTimeKey","dim":"[UsasSwimTime.UsasSwimTimeKey]","datatype":"numeric"}},
            {"jaql":{"title":"PersonKey","dim":"[UsasSwimTime.PersonKey]","datatype":"numeric", "filter":{"members":ids}}},
            {"jaql":{"title":"SwimEventKey","dim":"[UsasSwimTime.SwimEventKey]","datatype":"numeric"}}],
            "datasource":"USA Swimming Times Elasticube","by":"ComposeSDK","queryGuid":"2ba856aa-cd0a-4ea4-bcfc-89c03d52ffa8"})

    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    print(index)
    counter = 0
    while True:
        try:
            response = await session.post(url, headers=headers, data=payload, verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome", timeout=None)
            return response.json()
        except Exception as e:
            print(e)
            print(counter)
            time.sleep(30 + counter)
            counter += 1
