#from selenium import webdriver
import json
from MakeIDRequest import make_id_request
from GetProxies import get_proxies
from GetToken import get_token
from seleniumwire import webdriver
import pandas as pd
import psycopg
from psycopg import sql 
from get_credentials import get_credentials
from io import StringIO
import time

def send_data(data, cur, conn):
    columns = ['FirstName', 'MiddleName', 'LastName', 'Team', 'LSC', 'Age', 'PersonKey']

    placeholders = sql.SQL(', ').join(sql.SQL('%s') for _ in columns)

    query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier('ResultsSchema'),
        sql.Identifier('SwimmerIDs'),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        placeholders
    )

    # Convert DataFrame rows → list of tuples
    rows = list(data.itertuples(index=False, name=None))

    cur.executemany(query, rows)
    conn.commit()

def split_name(full_name):
    parts = full_name.split()
    
    if len(parts) == 1:                   # Only one name
        return parts[0], None, None
    
    if len(parts) == 2:                   # First + Last
        return parts[0], None, parts[1]
    
    # 3+ parts → First, Middle..., Last
    return parts[0], ' '.join(parts[1:-1]), parts[-1]

def get_ids():
    bearer_token = get_token()
    print(bearer_token)

    offset = 0
    row_list = []
    length_of_response = 10000
    count = 0
    while length_of_response >= 10000:
        response = make_id_request(bearer_token, 10000, offset).get("values")
        response = [tuple(data.get('text') for data in row) for row in response]
        row_list.extend(response)
        offset += 10000
        length_of_response = len(response)
        count += 1
        print(count)

    print('done')
    headers = ['Name', 'Club', 'LSC', 'Age', 'PersonKey']
    df = pd.DataFrame(row_list, columns=headers)
    df = df.rename(columns={'Club':'Team'})
    df[['FirstName', 'MiddleName', 'LastName']] = df['Name'].apply(lambda x: pd.Series(split_name(x)))
    df.drop(columns=['Name'], inplace=True)
    df = df[['FirstName', 'MiddleName', 'LastName', 'Team', 'LSC', 'Age', 'PersonKey']]
    df = df.drop_duplicates(subset=['PersonKey'])
    dbname, port, password, host = get_credentials()
    t = time.time()
    with psycopg.connect(f"dbname={dbname} port={port} user=swimrank_write host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            i = 0
            while i < len(df) - 10000: 
                temp = df[i:i+10000]
                send_data(temp, cur, conn)
                i += 10000
            temp = df[i:]
            send_data(temp, cur, conn)
    print('sending data time')
    print(time.time() - t)





