from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import json
import asyncio

async def make_id_results_request(session, token, ids, index):
    url = "https://usaswimming.sisense.com/api/datasources/USA%20Swimming%20Times%20Elasticube/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps({
    "metadata":[
        {"jaql":{
            "title":"Event",
            "dim":"[SwimEvent.EventCode]",
            "datatype":"text"}},
        {"jaql":{
            "title":"Swim Time",
            "dim":"[UsasSwimTime.SwimTimeFormatted]",
            "datatype":"text"}},
        {"jaql":{
            "title":"Age",
            "dim":"[UsasSwimTime.AgeAtMeetKey]",
            "datatype":"numeric"}},
        {"jaql":{
            "title":"Points",
            "dim":"[UsasSwimTime.PowerPoints]",
            "datatype":"numeric"}},
        {"jaql":{
            "title":"Time Standard",
            "dim":"[TimeStandard.TimeStandardName]",
            "datatype":"text"}},
        {"jaql":{
            "title":"Meet",
            "dim":"[Meet.MeetName]",
            "datatype":"text"}},
        {"jaql":{
            "title":"LSC",
            "dim":"[OrgUnit.Level3Code]",
            "datatype":"text"}},
        {"jaql":{
            "title":"Team",
            "dim":"[OrgUnit.Level4Name]",
            "datatype":"text"}},
        {"jaql":{
            "title":"Swim Date",
            "dim":"[SeasonCalendar.CalendarDate (Calendar)]",
            "datatype":"datetime","level":"days"},
            "format":{"mask":{"days":"MM/dd/yyyy"}}},
        {"jaql":{
            "title":"PersonKey",
            "dim":"[UsasSwimTime.PersonKey]",
            "datatype":"numeric"}},
        {"jaql":{
            "title":"SwimEventKey",
            "dim":"[UsasSwimTime.SwimEventKey]",
            "datatype":"numeric"}},
        {"jaql":{
            "title":"Sex",
            "dim":"[EventCompetitionCategory.TypeName]",
            "datatype":"text"}
            },
        {"jaql":{
            "title":"MeetKey",
            "dim":"[UsasSwimTime.MeetKey]",
            "datatype":"numeric"}},
        {"jaql":{
            "title":"UsasSwimTimeKey",
            "dim":"[UsasSwimTime.UsasSwimTimeKey]"
            ,"datatype":"numeric"}},
        {"jaql":{
            "title":"PersonKey",
            "dim":"[UsasSwimTime.PersonKey]",
            "datatype":"numeric",
            "filter":{"members":ids}, 
            "sort":""},"panel":"scope"}, 
    ],
    "datasource":{
        "title":"USA Swimming Times Elasticube",
        "live":False},
    "by":"ComposeSDK",
    "queryGuid":"c0dd446b-a0f8-452a-81a0-10b9f5eb7305"}
    )

    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    await asyncio.sleep(0.2*(index%100))
    response = await session.post(url, headers=headers, data=payload, verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome", timeout=40)
    return response.json()

    """response = requests.request("POST", url, headers=headers, data=payload, 
                                verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome").json()
    return response"""