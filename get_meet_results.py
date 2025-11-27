import json
from MakeMeetRequest import make_meet_keys_request
from MakeMeetResultRequest import make_meet_results_request
from GetToken import get_token
from seleniumwire import webdriver
import pandas as pd
import time
from datetime import datetime, timedelta
import numpy as np
import psycopg
from psycopg import sql
import asyncio
from curl_cffi.requests import AsyncSession
from get_credentials import get_credentials
from collections import defaultdict
import random
from update_rankings import send_season_ranking_query

def get_previous_week_dates(today_date):
    current_week_start = today_date - timedelta(days=today_date.isoweekday() - 1)
    previous_week_start = current_week_start - timedelta(weeks=1)
    previous_week_dates = []
    for i in range(7):
        previous_week_dates.append((previous_week_start + timedelta(days=i)).strftime("%Y-%m-%d"))
    return previous_week_dates

def get_personkeys():
    query = """SELECT "PersonKey", "Age" FROM "ResultsSchema"."SwimmerIDs" """
    db, port, password, host = get_credentials()
    with psycopg.connect(f"dbname={db} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    # Convert list of tuples → set of ints
    return {row[0:2] for row in rows}

def send_id_data_batch(rows):
    values_sql = ", ".join(
        cur.mogrify("(%s, %s, %s, %s, %s, %s)", (
            r['Name'], r['Team'], r['LSC'], r['Age'], r['PersonKey'], r['Sex']
        )).decode("utf-8")
        for r in rows
    )

    query = f"""
        INSERT INTO "ResultsSchema"."SwimmerIDs"
        ("Name", "Club", "LSC", "Age", "PersonKey", "Sex")
        VALUES {values_sql}
        ON CONFLICT ("PersonKey") DO NOTHING
    """

    db, port, password, host = get_credentials()

    with psycopg.connect(f"dbname={db} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def send_age_data_batch(rows):
    query = """
        UPDATE "ResultsSchema"."SwimmerIDs" AS s
        SET "Age" = v.age
        FROM (
            VALUES %s
        ) AS v(personkey, age)
        WHERE s."PersonKey" = v.personkey;
    """
    # Convert list of dicts to list of tuples
    values = [(r['PersonKey'], r['Age']) for r in rows]

    db, port, password, host = get_credentials()

    with psycopg.connect(f"dbname={db} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            psycopg.extras.execute_values(
                cur, query, values, template="(%s, %s)"
            )
        conn.commit()

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
                    INSERT INTO {}.{} ({}) 
                    VALUES ({})
                    ON CONFLICT ("UsasSwimTimeKey") DO NOTHING""").format(
                    sql.Identifier("ResultsSchema"),
                    sql.Identifier(table),
                    sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                    placeholders
                )
                cur.executemany(query, records)
                conn.commit()

async def fetch_meet_keys(bearer_token, start_dates):
    async with AsyncSession() as session:
        return await make_meet_keys_request(session, bearer_token, start_dates)

async def fetch_meet_results(bearer_token, meet_keys):
    async with AsyncSession() as session:
        return await make_meet_results_request(session, bearer_token, meet_keys)

# Asynchronous function to process requests concurrently
async def process_requests(bearer_token, keys, swimmers_per_request, session):
    tasks = []
    total = len(keys)
    index = 0
    
    while index < total:
        temp_keys = keys[index:index + swimmers_per_request]
        tasks.append(fetch_meet_keys(session, bearer_token, temp_keys, index))
        index += swimmers_per_request

    return await asyncio.gather(*tasks)

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


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    all_ids = get_personkeys()
    all_formatted_responses = []
    today = datetime.today()
    previous_week_dates = get_previous_week_dates(today)
    print(previous_week_dates)
    loop = asyncio.get_event_loop()
    meet_keys = []
    bearer_token = get_token()
    #print(bearer_token)
    meets = loop.run_until_complete(fetch_meet_keys(bearer_token, previous_week_dates))
    for meet in meets['values']:
        meet_key = meet[6]['data']
        meet_keys.append(int(meet_key))
    time.sleep(3)
    print(len(meet_keys))
    loop = asyncio.get_event_loop()
    index = 0
    responses = []
    while index < len(meet_keys):
        if index + 5 < len(meet_keys):
            keys = meet_keys[index:index+5]
            responses.append(loop.run_until_complete(fetch_meet_results(bearer_token, keys)))
            time.sleep(0.5+random.random())
        else:
            keys = meet_keys[index:]
            responses.append(loop.run_until_complete(fetch_meet_results(bearer_token, keys)))
            break
        index += 5

    db_columns = [
        "Name","Sex","Age","AgeGroup","Event","Place","Session","Points",
        "SwimDate","LSC","Team","Meet","SwimTime","Relay","TimeStandard",
        "MeetKey","UsasSwimTimeKey","PersonKey","SwimEventKey"
    ]
    formatted = build_records(responses, db_columns)
    grouped_dict = defaultdict(list)
    for item in formatted:
        personkey = item["PersonKey"]
        grouped_dict[personkey].append(item)

    new_id_rows = []
    age_updates = []
    print(len(grouped_dict))
    
        # all_ids is set of tuples: {(PersonKey, Age), ...}
    existing_keys = {pk for pk, age in all_ids}
    existing_age_pairs = all_ids  # already tuples

    new_id_rows = []
    age_updates = []

    for personkey, rows in grouped_dict.items():
        first = rows[0]
        name = first["Name"]
        age = first["Age"]
        team = first["Team"]
        sex = first["Sex"]
        lsc = first["LSC"]
        if personkey not in existing_keys:
            print("adding id")
            new_id_rows.append({
                'Name': name,
                'Team': team,
                'LSC': lsc,
                'Age': age,
                'PersonKey': personkey,
                'Sex': sex
            })

        if (personkey, age) not in existing_age_pairs:
            print("adding age")
            age_updates.append({'PersonKey': personkey, 'Age': age})

        if new_id_rows:
            print("Inserting", len(new_id_rows), "new swimmers…")
            send_id_data_batch(new_id_rows)
        if age_updates:
            send_age_data_batch(age_updates)

        df = pd.DataFrame.from_dict(formatted)
        send_data(df)

    month = datetime.now().month
    year = datetime.now().year
    if month  >= 9:
        season_start = str(year) + "-09-01"
        season_end = str(year + 1) + "-09-01"
        season = f"{year}-{year + 1}"
    else:
        season_start = str(year - 1) + "-09-01"
        season_end = str(year) + "-09-01"
        season = f"{year - 1}-{year}"
    send_season_ranking_query(season_start, season_end, season)


            
