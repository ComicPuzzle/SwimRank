import asyncpg
import asyncio
from datetime import datetime
from get_credentials import get_credentials
import pandas as pd
import time

global_pool = None 

def get_age_group(age):
    a1 = 0
    a2 = 10
    if age <= 10:
        pass
    elif age <= 12:
        a1, a2 = 11, 12
    elif age <= 14:
        a1, a2 = 13, 14
    elif age <= 16:
        a1, a2 = 15, 16
    elif age <= 18:
        a1, a2 = 17, 18
    else:
        a1, a2 = 19, 99
    return (a1, a2)

def age_group_str(age):
    return f"{age[0]}-{age[1]}"

def convert_timedelta(val):
    minutes = int(val.total_seconds() // 60)
    if minutes == 0:
        seconds = val.total_seconds()
        return f"{round(seconds, 2):.2f}"
    else:
        seconds = val.total_seconds() - 60 * minutes
        return f"{minutes}:{round(seconds, 2):05.2f}"

async def get_global_pool(dbname, ip, port, password):
    """Return a shared asyncpg pool for all requests."""
    global global_pool
    if global_pool is None or global_pool.is_closing():
        global_pool = await asyncpg.create_pool(
            dsn=f'postgres://postgres:{password}@{ip}:{port}/{dbname}', #change to remote address
            max_inactive_connection_lifetime=20,
            min_size=1,
            max_size=10,  # adjust based on server capacity
        )
    return global_pool

async def fetch_person_season_rank(season_start_year, table, dbname, ip, port, password, sex, age):
    a = get_age_group(age)
    global global_pool
    query = f"""SELECT
                    ANY_VALUE(r.event) AS event,
                    r.personkey,
                    p."Name" AS name, 
                    r.sex,
                    r.age,
                    ANY_VALUE(r.team) AS team,
                    ANY_VALUE(r.lsc) AS lsc,
                    ANY_VALUE(r.usasswimtimekey) AS usasswimtimekey,
                    ANY_VALUE(r.meet) AS meet,
                    ANY_VALUE(r.swimdate) AS swimdate,
                    ANY_VALUE(r.relay) AS relay,
                    MIN(r.swimtime) AS swimtime,
                    RANK() OVER (ORDER BY MIN(r.swimtime)) AS rank
                FROM
                    "ResultsSchema"."{table}" AS r
                JOIN
                    "ResultsSchema"."SwimmerIDs" AS p
                    ON r.personkey = p."PersonKey"
                WHERE
                    r.sex = {sex}
                    AND r.age BETWEEN {a[0]} AND {a[1]}
                    AND r.swimdate >= DATE '{season_start_year-1}-09-01'
                    AND r.swimdate <  DATE '{season_start_year}-09-01'
                GROUP BY
                    r.personkey, p."Name", r.sex, r.age
                ORDER BY
                    swimtime
                LIMIT 1000"""
    #pool = await get_global_pool(dbname, ip, port, password)
    async with global_pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def collect_all_ranking_data(dbname, ip, port, password):
    
    if datetime.now().month >= 9:
        season_start_year = datetime.now().year
    else:
        season_start_year = datetime.now().year - 1 
    table_name = str(season_start_year) + '_' + str(season_start_year +1) + '_rankings'
    conn = await asyncpg.connect(f'postgres://postgres:{password}@{ip}:{port}/{dbname}')
    print('got connection')
    await conn.execute(f'DROP TABLE IF EXISTS "ResultsSchema"."{table_name}"')
    await conn.close()
    print('deleted table')
    db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
    tasks = []
    print("start" + str(time.time()))
    for s in [0, 1]:
        for a in [10, 12, 14, 16, 18, 20]:
            for table in db_table_names:
                tasks.append(fetch_person_season_rank(season_start_year, table, dbname, ip, port, password, s, a))
    results = await asyncio.gather(*tasks)
    season_ranking_data = [item for sublist in results if sublist for item in sublist]
    print("end" + str(time.time()))
    return season_ranking_data

async def create_table(dbname, ip, port, password, table_name, df):
    conn = await asyncpg.connect(f'postgres://postgres:{password}@{ip}:{port}/{dbname}')

    try:
        # Create table based on DataFrame schema
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (event text, personkey integer, name text, sex integer, age text, team text, lsc text, usasswimtimekey integer, meet text, swimdate timestamp without time zone, swimtime interval, national_rank integer, age_group text, lsc_rank integer, club_rank integer)"
        print(create_table_query)
        await conn.execute(create_table_query)
        
        # Insert data using copy_records_to_table
        tuples = [tuple(x) for x in df.values]
        print(tuples)
        await conn.copy_records_to_table(
            table_name,
            records=tuples,
            columns=list(df.columns),
            timeout=10
        )
    finally:
        await conn.close()

async def main():
    global global_pool
    dbname, port, password, ip = get_credentials()
    await get_global_pool(dbname, ip, port, password)
    """season_ranking_data = await collect_all_ranking_data(dbname, ip, port, password)
   
    national_rank_data_df = pd.DataFrame(season_ranking_data, columns=["event" ,"name", "personkey", "sex", "age", "team", "lsc", "usasswimtimekey", "meet", "swimdate", "relay", "swimtime", "rank"])
    national_rank_data_df["swimtime"] = national_rank_data_df.apply(lambda row: convert_timedelta(row['swimtime']) + "r" if row['relay'] == 1 else convert_timedelta(row['swimtime']), axis=1)
    national_rank_data_df["swimdate"] = national_rank_data_df["swimdate"].apply(lambda x: x.strftime('%m/%d/%Y'))
    national_rank_data_df['age'] = national_rank_data_df['age'].apply(lambda x: get_age_group(x))
    national_rank_data_df['age group'] = national_rank_data_df['age'].apply(lambda x: age_group_str(x))
    national_rank_data_df.rename(columns={'rank': 'national rank'}, inplace=True)
    national_rank_data_df.drop('relay', axis=1, inplace=True)
    national_rank_data_df['lsc rank'] = national_rank_data_df.groupby(['sex', 'age group', 'event', 'lsc'])['swimtime'].rank(method='dense', ascending=True)
    national_rank_data_df['club rank'] = national_rank_data_df.groupby(['sex', 'age group', 'event', 'team'])['swimtime'].rank(method='dense', ascending=True)
    national_rank_data_df.to_csv('national_rankings.csv', index=False)
    """
    national_rank_data_df = pd.read_csv('national_rankings.csv')
    national_rank_data_df.rename(columns={'name': 'personke', 'personkey':'name'}, inplace=True)
    national_rank_data_df.rename(columns={'personke':'personkey'}, inplace=True)
    
    if datetime.now().month >= 9:
        season_start_year = datetime.now().year
    else:
        season_start_year = datetime.now().year - 1
    table_name = f'"ResultsSchema"."{season_start_year}_{season_start_year+1}_rankings"'

    await create_table(dbname, ip, port, password, table_name, national_rank_data_df)

if __name__ == '__main__':
    asyncio.run(main())