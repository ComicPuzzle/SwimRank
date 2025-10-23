#from selenium import webdriver
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

def send_data(data):
    dbname, port, password = get_credentials()
    with psycopg.connect(f"dbname={dbname} port={port} user=postgres host='localhost' password='{password}'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            event_columns = ['50 FR SCY', '50 FR LCM', '100 FR SCY', '100 FR LCM',
                        '200 FR SCY', '200 FR LCM', '400 FR LCM', '500 FR SCY', 
                        '800 FR LCM', '1000 FR SCY', '1500 FR LCM', '1650 FR SCY',
                        '50 BK SCY', '100 BK SCY', '200 BK SCY', '50 BK LCM', '100 BK LCM', '200 BK LCM',
                        '50 FL SCY', '100 FL SCY', '200 FL SCY', '50 FL LCM', '100 FL LCM', '200 FL LCM',
                        '50 BR SCY', '100 BR SCY', '200 BR SCY', '50 BR LCM', '100 BR LCM', '200 BR LCM',
                        '100 IM SCY', '200 IM SCY', '400 IM SCY', '200 IM LCM', '400 IM LCM'
                        ]
            db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
            db_columns = [  "event",
                            "swimtime",
                            "relay",
                            "age",
                            "points",
                            "timestandard",
                            "meet",
                            "lsc",
                            "team",
                            "swimdate",
                            "personkey",
                            "swimeventkey",
                            "sex",
                            "meetkey",
                            "usasswimtimekey"]
            for i in range(len(event_columns)):
                temp_df = data.loc[data['event'] == event_columns[i]]
                temp = temp_df.to_dict(orient='records')
                query = sql.SQL("INSERT INTO {""}.{""} ({}) VALUES ({})").format(
                        sql.Identifier('ResultsSchema'),
                        sql.Identifier(db_table_names[i]),
                        sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                        sql.SQL(', ').join(map(sql.Placeholder, db_columns))             
                    )
                #print(temp)
                cur.executemany(query, temp)
                conn.commit()

async def fetch_id_results(session, bearer_token, temp_keys, index):
    return await make_id_results_request(session, bearer_token, temp_keys, index)

# Asynchronous function to process requests concurrently
async def process_requests(bearer_token, keys, swimmers_per_request, session):
    tasks = []
    total = len(keys)
    index = 0
    
    while index < total:
        temp_keys = keys[index:index + swimmers_per_request]
        tasks.append(fetch_id_results(session, bearer_token, temp_keys, index))
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
    # Convert mm/dd/yyyy to yyyy-mm-dd and append time
    time_string = "00:00:00"
    month, day, year = date_str.split('/')
    return f"{year}-{month}-{day} {time_string}"

async def main_func(keys, bearer_token):
    async with AsyncSession() as s:
        swimmers_per_request = 50
        responses = await process_requests(bearer_token, keys, swimmers_per_request, s)
        #print("got responses")
        formatted_response = []
        db_columns = [  "event",
                        "swimtime",
                        "relay",
                        "age",
                        "points",
                        "timestandard",
                        "meet",
                        "lsc",
                        "team",
                        "swimdate",
                        "personkey",
                        "swimeventkey",
                        "sex",
                        "meetkey",
                        "usasswimtimekey",]
        for response in responses:
            for event in response["values"]:
                temp = []
                for i in range(len(event)):
                    data = event[i]['data']
                    if i == 1:
                        t, r = convert_to_interval(data)
                        temp.append(t)
                        temp.append(r)
                    elif i == 3:
                        try:
                            temp.append(int(data))
                        except:
                            temp.append(0)
                    elif i in [2, 9, 10, 12, 13, 14]:
                        temp.append(int(data))
                    elif i == 8:
                        data = event[i]['text']
                        temp.append(convert_to_timestamp(data))
                    elif i == 11:
                        if data == "Male":
                            temp.append(0)
                        else:
                            temp.append(1)
                    else:
                        temp.append(data)
                formatted_response.append(dict(zip(db_columns, temp)))
        return formatted_response

def remove_first_n_lines(filename, n):
    try:
        # Read the file
        with open(filename, 'r') as file:
            lines = file.readlines()
        
        # Check if there are enough lines to remove
        if n >= len(lines):
            print(f"Warning: File has only {len(lines)} lines. All lines will be removed.")
            n = len(lines)
        
        # Write the file back, excluding the first n lines
        with open(filename, 'w') as file:
            file.writelines(lines[n:])
        
        print(f"Successfully removed the first {n} lines from {filename}")
    except IOError:
        print(f"An error occurred while processing the file {filename}")


if __name__ == "__main__":
    try:
        keys = []
        with open("ids.txt", "r") as file:
            lines = file.readlines()
            for key in lines:
                #key = json.loads(person).get("PersonKey")
                keys.append(int(key))
        total = len(keys)
        i = 0
        loop = asyncio.get_event_loop()
        all_formatted_responses = []
        bearer_token = get_token()
        print("bearer token retrieved")
        print(time.time())
        while i < total:   
            if i + 500 >= total:
                k = keys[i:] 
            else:
                k = keys[i:i+500]
            if i % 15000 == 0 and i != 0:
                payload = pd.DataFrame.from_dict(all_formatted_responses)
                send_data(payload)
                print("Data Sent")
                all_formatted_responses = []
                bearer_token = get_token()
                print("bearer token retrieved")
                remove_first_n_lines('ids.txt', 15000)
            all_swimmers = loop.run_until_complete(main_func(k, bearer_token))
            all_formatted_responses.extend(all_swimmers)
            i += 500
            print(i)
        payload = pd.DataFrame.from_dict(all_formatted_responses)
        send_data(payload)
        print("Final Data Sent")
        print(time.time())
        
        loop.close()
    except Exception as e:
        print(e)