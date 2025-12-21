from curl_cffi import requests
import json
import time
def make_ncaa_request(token, event, gender, divison, season):
    url = "https://usaswimming.sisense.com/api/datasources/Public%20Person%20Search/jaql?trc=sdk-ui-1.23.0"

    payload = json.dumps(
    {"metadata":[
    {"jaql":{"title":"SwimTime","dim":"[NcaaSwimTime.SwimTimeFormatted]","datatype":"text"}},
    {"jaql":{"title":"SwimEventKey","dim":"[NcaaSwimTime.SwimEventKey]","datatype":"numeric"}},
    {"jaql":{"title":"TypeName","dim":"[EventCompetitionCategory.TypeName]","datatype":"text"}},
    {"jaql":{"title":"EventCompetitionCategoryKey","dim":"[NcaaSwimTime.EventCompetitionCategoryKey]","datatype":"numeric"}},
    {"jaql":{"title":"NCAASeason","dim":"[SeasonCalendar.NCAASeason]","datatype":"text"}},
    {"jaql":{"title":"EventCode","dim":"[SwimEvent.EventCode]","datatype":"text"}},
    {"jaql":{"title":"SwimTimeSeconds","dim":"[NcaaSwimTime.SwimTimeSeconds]","datatype":"numeric"}},
    {"jaql":{"title":"SortKey","dim":"[NcaaSwimTime.SortKey]","datatype":"text","sort":"asc"}},
    {"jaql":{"title":"NcaaSwimTimeKey","dim":"[NcaaSwimTime.NcaaSwimTimeKey]","datatype":"numeric"}},
    {"jaql":{"title":"Rank","formula":"RANK(min([C2A1A-9AA]),\"ASC\",\"1224\", [7BAEA-79D],[339F5-77E],[024F1-F1B])","context":{"[339F5-77E]":{"title":"EventCompetitionCategoryKey","dim":"[NcaaSwimTime.EventCompetitionCategoryKey]","datatype":"numeric"},"[C2A1A-9AA]":{"title":"SwimTimeSeconds","dim":"[NcaaSwimTime.SwimTimeSeconds]","datatype":"numeric"},"[7BAEA-79D]":{"title":"SwimEventKey","dim":"[NcaaSwimTime.SwimEventKey]","datatype":"numeric"},"[024F1-F1B]":{"title":"NCAASeason","dim":"[SeasonCalendar.NCAASeason]","datatype":"text"}}}},
    {"jaql":{"title":"Division","dim":"[OrgUnit.Division]","datatype":"text","filter":{"equals":divison}},"panel":"scope"},
    {"jaql":{"title":"NCAASeason","dim":"[SeasonCalendar.NCAASeason]","datatype":"text","filter":{"members":[season]}},"panel":"scope"},
    {"jaql":{"title":"TypeName","dim":"[EventCompetitionCategory.TypeName]","datatype":"text","filter":{"members":[gender]}},"panel":"scope"},
    {"jaql":{"title":"EventCode","dim":"[SwimEvent.EventCode]","datatype":"text","filter":{"members":[event]}},"panel":"scope"},
    {"jaql":{"title":"SeasonBest","dim":"[NcaaSwimTime.SeasonBest]","datatype":"text","filter":{"equals":True}},"panel":"scope"},
    {"jaql":{"title":"Ineligible","dim":"[NcaaSwimTime.Ineligible]","datatype":"text","filter":{"equals":False}},"panel":"scope"}],
    "datasource":"NCAA Times","by":"ComposeSDK","queryGuid":"dece1d8a-1ae5-4946-9252-d04494ca37d0"})

    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = None

    while response == None:
        response = requests.request("POST", url, headers=headers, data=payload, 
                                verify=["/Users/daniel/Desktop/others -imp/SwimRank/_.usaswimming.org.pem", "/Users/daniel/Desktop/others -imp/SwimRank/cacert.pem"], 
                                        impersonate="chrome").json()

    return response