import json
from MakeIDResultRequests import make_rankings_results_request
from GetToken import get_token
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
            columns = ['Event', 'ByClub', 'ByLSC', 'ByZone', 'ByNation']
            query = sql.SQL("INSERT INTO {""}.{""} ({}) VALUES ({})").format(
                    sql.Identifier('ResultsSchema'),
                    sql.Identifier('SwimmerEventResults'),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(map(sql.Placeholder, columns))             
                )
            cur.executemany(query, data)
            conn.commit()


async def fetch_id_results(session, bearer_token, temp_keys, index):
    return await make_rankings_results_request(session, bearer_token, temp_keys, index)

# Asynchronous function to process requests concurrently
async def process_requests():
    pass


async def main_func(keys, bearer_token):
    pass

if __name__ == "__main__":
    pass