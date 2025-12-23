import json
from MakeIDResultRequests import make_id_results_request
from GetToken import get_token
from seleniumwire import webdriver
import pandas as pd
import time
import numpy as np
import psycopg
from psycopg import sql
import asyncio
from curl_cffi.requests import AsyncSession
from get_credentials import get_credentials
import random
from get_rankings_once import send_rankings_query

SEM = asyncio.Semaphore(10)

def get_personkeys():
    query = """SELECT "PersonKey" FROM "ResultsSchema"."SwimmerIDs" """
    db, port, password, host = get_credentials()
    with psycopg.connect(f"dbname={db} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    # Convert list of tuples → set of ints
    return [row[0] for row in rows]

def send_data(df):
    db, port, password, host = get_credentials()
    table_map = {
        '50 FR SCY': '50_FR_SCY_results',
        '50 FR LCM': '50_FR_LCM_results',
        '100 FR SCY': '100_FR_SCY_results',
        '100 FR LCM': '100_FR_LCM_results',
        '200 FR SCY': '200_FR_SCY_results',
        '200 FR LCM': '200_FR_LCM_results',
        '400 FR LCM': '400_FR_LCM_results',
        '500 FR SCY': '500_FR_SCY_results',
        '800 FR LCM': '800_FR_LCM_results',
        '1000 FR SCY': '1000_FR_SCY_results',
        '1500 FR LCM': '1500_FR_LCM_results',
        '1650 FR SCY': '1650_FR_SCY_results',
        '50 BK SCY': '50_BK_SCY_results',
        '100 BK SCY': '100_BK_SCY_results',
        '200 BK SCY': '200_BK_SCY_results',
        '50 BK LCM': '50_BK_LCM_results',
        '100 BK LCM': '100_BK_LCM_results',
        '200 BK LCM': '200_BK_LCM_results',
        '50 FL SCY': '50_FL_SCY_results',
        '100 FL SCY': '100_FL_SCY_results',
        '200 FL SCY': '200_FL_SCY_results',
        '50 FL LCM': '50_FL_LCM_results',
        '100 FL LCM': '100_FL_LCM_results',
        '200 FL LCM': '200_FL_LCM_results',
        '50 BR SCY': '50_BR_SCY_results',
        '100 BR SCY': '100_BR_SCY_results',
        '200 BR SCY': '200_BR_SCY_results',
        '50 BR LCM': '50_BR_LCM_results',
        '100 BR LCM': '100_BR_LCM_results',
        '200 BR LCM': '200_BR_LCM_results',
        '100 IM SCY': '100_IM_SCY_results',
        '200 IM SCY': '200_IM_SCY_results',
        '400 IM SCY': '400_IM_SCY_results',
        '200 IM LCM': '200_IM_LCM_results',
        '400 IM LCM': '400_IM_LCM_results'
    }

    db_columns = [
        "Name","Sex","Age","AgeGroup","Event","Place","Session","Points",
        "SwimDate","LSC","Team","Meet","SwimTime","Relay","TimeStandard",
        "MeetKey","UsasSwimTimeKey","PersonKey","SwimEventKey"
    ]

    with psycopg.connect(f"dbname={db} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            # Pre-split entire dataframe by event (VERY Fast)
            grouped = {event: df[df["Event"] == event] for event in table_map}
            for event, table in table_map.items():
                temp_df = grouped[event]
                if temp_df.empty:
                    continue

                #records = temp_df.to_dict(orient="records")
                records = list(temp_df.itertuples(index=False))
                placeholders = sql.SQL(', ').join(sql.SQL('%s') for _ in db_columns)
                query = sql.SQL("""
                    INSERT INTO {}.{} ({}) VALUES ({})""").format(
                    sql.Identifier("ResultsSchema"),
                    sql.Identifier(table),
                    sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                    placeholders
                )
                cur.executemany(query, records)
                conn.commit()

async def fetch_id_results(session, bearer_token, temp_keys, index):
    async with SEM:                   
        return await make_id_results_request(session, bearer_token, temp_keys, index)

# Asynchronous function to process requests concurrently
async def process_requests(session, bearer_token, keys, swimmers_per_request):
    tasks = []
    total = len(keys)
    request_count = 0
    
    responses = []

    for i in range(0, total, swimmers_per_request):

        # Refresh bearer token every 1000 requests
        if request_count > 0 and request_count % 1000 == 0:
            print(f"Refreshing bearer token at request #{request_count}")
            bearer_token = get_token()
            print("New bearer token retrieved")

        key_slice = keys[i:i + swimmers_per_request]

        # one single request
        result = await fetch_id_results(session, bearer_token, key_slice, i)
        responses.append(result)

        request_count += 1

    return responses

def convert_to_interval(time_str):
    if "r" in time_str:
        t, _ = time_str.split("r")
        try:
            minutes, seconds = t.split(':')
            seconds, milliseconds = seconds.split('.')
            return (f"{int(minutes)} minutes {int(seconds)}.{milliseconds} seconds", 1)
        except:
            seconds, milliseconds = t.split('.')
            return (f"{int(seconds)}.{milliseconds} seconds", 1)
    else:
        try:
            minutes, seconds = time_str.split(':')
            seconds, milliseconds = seconds.split('.')
            return (f"{int(minutes)} minutes {int(seconds)}.{milliseconds} seconds", 0)
        except:
            seconds, milliseconds = time_str.split('.')
            return (f"{int(seconds)}.{milliseconds} seconds", 0)

def convert_to_timestamp(date_str):
    # Convert mm/dd/yyyy to yyyy-mm-dd and append time
    time_string = "00:00:00"
    month, day, year = date_str.split('/')
    return f"{year}-{month}-{day} {time_string}"

def build_records(responses, db_columns):
    out = []

    for r in responses:
        for ev in r["values"]:
            row = [None]*19

            # direct list lookup is fastest
            row[0] = ev[0]["data"]
            row[1] = 0 if ev[1]["data"]=="Male" else 1
            row[2] = int(ev[2]["data"])
            if row[2] == 15 or row[2] == 16:
                row[3] = "15-16"
            elif row[2] == 17 or row[2] == 18:
                row[3] = "17-18"
            else:
                row[3] = ev[3]["data"]

            row[4] = ev[4]["data"]
            row[5] = int(ev[5]["data"]) if type(ev[5]["data"]) == int else -1
            row[6] = ev[6]["data"]

            try:
                row[7] = int(ev[7]["data"])
            except:
                row[7] = 0

            # date text
            row[8] = convert_to_timestamp(ev[8]["text"])

            # LSC, Team, Meet
            row[9] = ev[9]["data"]
            row[10] = ev[10]["data"]
            row[11] = ev[11]["data"]

            # Time
            t, r = convert_to_interval(ev[12]["data"])
            row[12] = t
            row[13] = r
            # Relay, TimeStandard, MeetKey, UsasKey, PersonKey, SwimEventKey
            row[14] = ev[13]["data"]
            row[15] = int(ev[14]["data"])
            row[16] = int(ev[15]["data"])
            row[17] = int(ev[16]["data"])
            row[18] = int(ev[17]["data"])

            out.append(dict(zip(db_columns, row)))

    return out

async def main_func(keys, bearer_token):
    async with AsyncSession() as s:
        swimmers_per_req = 40
        responses = await process_requests(s, bearer_token, keys, swimmers_per_req)
        #print("got responses")
        #formatted_response = []
        db_columns = ["Name", "Sex", "Age", "AgeGroup", "Event", "Place", "Session", "Points", "SwimDate", "LSC", "Team",
                    "Meet", "SwimTime", "Relay", "TimeStandard", "MeetKey", "UsasSwimTimeKey", "PersonKey", "SwimEventKey"]
        formatted = build_records(responses, db_columns)
        return formatted

async def process_chunk(session, bearer_token, keys, swimmers_per_request):
    tasks = [
        fetch_id_results(session, bearer_token, keys[i:i+swimmers_per_request], i)
        for i in range(0, len(keys), swimmers_per_request)
    ]
    return await asyncio.gather(*tasks)


async def run_all_requests(keys):
    swimmers_per_request = 40
    requests_per_token = 1000
    swimmers_per_token = swimmers_per_request * requests_per_token  # = 40k

    all_responses = []

    async with AsyncSession() as session:

        for start in range(0, len(keys), swimmers_per_token):

            end = start + swimmers_per_token
            key_slice = keys[start:end]

            # New token for each 40,000 swimmers
            bearer_token = get_token()
            print(f"Generated bearer token for swimmers {start}–{end}")

            responses = await process_chunk(
                session,
                bearer_token,
                key_slice,
                swimmers_per_request
            )

            all_responses.extend(responses)

    return all_responses


def get_id_results():
    keys = get_personkeys()
    loop = asyncio.get_event_loop()
    keys = keys[:40000]
    responses = loop.run_until_complete(run_all_requests(keys))

    formatted = build_records(responses, [
        "Name","Sex","Age","AgeGroup","Event","Place","Session","Points",
        "SwimDate","LSC","Team","Meet","SwimTime","Relay","TimeStandard",
        "MeetKey","UsasSwimTimeKey","PersonKey","SwimEventKey"
    ])
    t = time.time()
    df = pd.DataFrame.from_dict(formatted)
    send_data(df)
    print("Final Data Sent")
    print(time.time() - t)
    loop.close()
