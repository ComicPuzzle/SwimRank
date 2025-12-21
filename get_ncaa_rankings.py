#from selenium import webdriver
import json
from MakeNCAARequest import make_ncaa_request
from GetProxies import get_proxies
from GetToken import get_token
from seleniumwire import webdriver
import pandas as pd
import psycopg
from psycopg import sql 
from get_credentials import get_credentials
from io import StringIO
from datetime import datetime 
import time

def convert_to_interval(time_str):
    if "r" in time_str:
        time_str, _ = time_str.split("r")
    try:
        minutes, seconds = time_str.split(':')
        seconds, milliseconds = seconds.split('.')
        return f"{int(minutes)} minutes {int(seconds)}.{milliseconds} seconds"
    except:
        seconds, milliseconds = time_str.split('.')
        return f"{int(seconds)}.{milliseconds} seconds"


def get_ncaa_rankings():
    bearer_token = get_token()
    current_month = datetime.now().month
    current_year = datetime.now().year
    if current_month >= 9:
        current_season = str(current_year) + "-"  + str(current_year+1)
    else:
        current_season = str(current_year-1) + "-"  + str(current_year)

    dbname, port, password, host = get_credentials()
    tables = ["DivI_Male",  "DivI_Female",  "DivII_Male",  "DivII_Female",  "DivIII_Male",  "DivIII_Female"]
    c = ["Event", "Sex", "SwimTime", "NcaaSwimTimeKey"]
    f = ["text", "text", "interval", "integer"]
    k = "NcaaSwimTimeKey"   

    with psycopg.connect(f"dbname={dbname} port={port} user=postgres host={host} password={password}") as conn:
        with conn.cursor() as cur:
            for table_name in tables:
                drop_query = sql.SQL("""DROP TABLE IF EXISTS {schema}.{table} CASCADE;""").format(
                    schema=sql.Identifier("ResultsSchema"),
                    table=sql.Identifier(table_name)
                )
                cur.execute(drop_query)
                col_defs = [sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(fmt)) for col, fmt in zip(c, f)]

                create_query = sql.SQL("""
                        CREATE TABLE {schema}.{table} ({columns}, CONSTRAINT {pkey} PRIMARY KEY ({key}));""").format(
                        schema=sql.Identifier("ResultsSchema"),
                        table=sql.Identifier(table_name),
                        columns=sql.SQL(", ").join(col_defs),
                        pkey=sql.Identifier(f"{table_name}_pkey"),
                        key = sql.Identifier(k)
                    )

                cur.execute(create_query)

                index_query = f"""CREATE INDEX "{table_name + "_event_idx"}" ON "ResultsSchema"."{table_name}"  USING btree ("Event") WITH (deduplicate_items=True);"""
                cur.execute(index_query)

                conn.commit()

    events = ['50 FR SCY', '100 FR SCY', '200 FR SCY', '500 FR SCY', '1000 FR SCY', '1650 FR SCY',
        '50 FL SCY', '100 FL SCY', '200 FL SCY', '50 BK SCY', '100 BK SCY', '200 BK SCY', 
        '50 BR SCY', '100 BR SCY', '200 BR SCY', '100 IM SCY', '200 IM SCY', '400 IM SCY']

    for division in ["NCAA Div I", "NCAA Div II", "NCAA Div III"]:
        for gender in ["Male", "Female"]:
            row_list = []
            for event in events:
                print(event)
                response = make_ncaa_request(bearer_token, event, gender, division, current_season)
                response = response.get("values")
                response = [tuple(data.get('text') for data in row) for row in response]
                row_list.extend(response)
            headers = ["SwimTime", "SwimEventKey", "TypeName", "EventCompetitionCategoryKey", "NCAASeason", "EventCode", "SwimTimeSeconds", "SortKey", "NcaaSwimTimeKey", "Rank"]
            df = pd.DataFrame(row_list, columns=headers)
            df = df.rename(columns={'EventCode':'Event', 'TypeName':'Sex'})
            df = df[['Event', 'Sex', 'SwimTime', 'NcaaSwimTimeKey']]
            df['SwimTime'] = df['SwimTime'].apply(lambda x: convert_to_interval(x))
          
            table_name = ""
            if division == "NCAA Div I":
                table_name += "DivI_"
            elif division == "NCAA Div II":
                table_name += "DivII_"
            else:
                table_name += "DivIII_"
            if gender == "Male":
                table_name += "Male"
            else:
                table_name += "Female"
            print(table_name)

            with psycopg.connect(f"dbname={dbname} port={port} user=postgres host='{host}' password='{password}'") as conn:
                with conn.cursor() as cur:
                        db_columns = ['Event', 'Sex', 'SwimTime', 'NcaaSwimTimeKey']
                        records = list(df.itertuples(index=False))
                        placeholders = sql.SQL(', ').join(sql.SQL('%s') for _ in db_columns)
                        query = sql.SQL("""
                            INSERT INTO {}.{} ({}) VALUES ({})""").format(
                            sql.Identifier("ResultsSchema"),
                            sql.Identifier(table_name),
                            sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                            placeholders
                        )
                        cur.executemany(query, records)
                        conn.commit()


if __name__ == "__main__":
    get_ncaa_rankings()