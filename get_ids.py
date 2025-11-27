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


def send_data(data, cur, conn):
    columns = ['Name', 'Club', 'LSC', 'Age', 'PersonKey']

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

if __name__ == "__main__":
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

    headers = ['Name', 'Club', 'LSC', 'Age', 'PersonKey']
    df = pd.DataFrame(row_list, columns=headers)
    #print(df.head(5))
    #df.to_json('ids.json', orient='records', lines=True)
    dbname, port, password, host = get_credentials()
    with psycopg.connect(f"dbname={dbname} port={port} user=postgres host='{host}' password='{password}'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            i = 0
            while i < len(df) - 10000: 
                temp = df[i:i+10000]
                send_data(temp, cur, conn)
                i += 10000
            temp = df[i:]
            send_data(temp, cur, conn)




