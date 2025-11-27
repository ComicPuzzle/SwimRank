import json
from GetToken import get_token
import pandas as pd
import time
import numpy as np
import psycopg
from psycopg import sql
import asyncio
from curl_cffi.requests import AsyncSession
from get_credentials import get_credentials

def send_rankings_query():
    dbname, port, password, host = get_credentials()
    table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results',
        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results',
        '100_BK_LCM_results', '200_BK_LCM_results', '50_FL_SCY_results', '100_FL_SCY_results',
        '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results', 
        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', 
        '100_BR_LCM_results', '200_BR_LCM_results', '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
    ]
    with psycopg.connect(f"dbname={dbname} port={port} user=postgres host='{host}' password='{password}'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            for table in table_names:
                query = f"""
                    WITH season_data AS (
                    SELECT 
                        r."UsasSwimTimeKey",
                        r."PersonKey",
                        r."Sex",
                        r."AgeGroup",
                        r."LSC",
                        r."Team",
                        r."SwimDate",
                        r."SwimTime",
                        CASE
                            WHEN r."SwimDate" >= '2025-09-01' AND r."SwimDate" < '2026-09-01' THEN '2025-2026'
                            WHEN r."SwimDate" >= '2024-09-01' AND r."SwimDate" < '2025-09-01' THEN '2024-2025'
                            WHEN r."SwimDate" >= '2023-09-01' AND r."SwimDate" < '2024-09-01' THEN '2023-2024'
                            WHEN r."SwimDate" >= '2022-09-01' AND r."SwimDate" < '2023-09-01' THEN '2022-2023'
                            WHEN r."SwimDate" >= '2021-09-01' AND r."SwimDate" < '2022-09-01' THEN '2021-2022'
                            WHEN r."SwimDate" >= '2020-09-01' AND r."SwimDate" < '2021-09-01' THEN '2020-2021'
                            WHEN r."SwimDate" >= '2019-09-01' AND r."SwimDate" < '2020-09-01' THEN '2019-2020'
                            WHEN r."SwimDate" >= '2018-09-01' AND r."SwimDate" < '2019-09-01' THEN '2018-2019'
                            WHEN r."SwimDate" >= '2017-09-01' AND r."SwimDate" < '2018-09-01' THEN '2017-2018'
                            WHEN r."SwimDate" >= '2016-09-01' AND r."SwimDate" < '2017-09-01' THEN '2016-2017'
                        END AS season
                    FROM "ResultsSchema"."{table}" r
                ),

                best_times AS (
                    SELECT 
                        "PersonKey",
                        "Sex",
                        "AgeGroup",
                        "LSC",
                        "Team",
                        season,
                        MIN("SwimTime") AS best_time
                    FROM season_data
                    GROUP BY "PersonKey", "Sex", "AgeGroup", "LSC", "Team", season
                ),

                ranked AS (
                    SELECT 
                        b.*,
                        RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup", b.season
                            ORDER BY b.best_time
                        ) AS national_rank,

                        RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup", b.season, b."LSC"
                            ORDER BY b.best_time
                        ) AS lsc_rank,

                        RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup", b.season, b."Team"
                            ORDER BY b.best_time
                        ) AS club_rank
                    FROM best_times b
                ),

                final_ranks AS (
                    SELECT 
                        s."UsasSwimTimeKey",
                        CASE WHEN s."SwimTime" = r.best_time THEN r.national_rank ELSE -1 END AS national_rank,
                        CASE WHEN s."SwimTime" = r.best_time THEN r.lsc_rank      ELSE -1 END AS lsc_rank,
                        CASE WHEN s."SwimTime" = r.best_time THEN r.club_rank     ELSE -1 END AS club_rank
                    FROM season_data s
                    LEFT JOIN ranked r
                        ON s."PersonKey" = r."PersonKey"
                        AND s."Sex" = r."Sex"
                        AND s."AgeGroup" = r."AgeGroup"
                        AND s.season = r.season
                        AND s."LSC" = r."LSC"
                        AND s."Team" = r."Team"
                )

                UPDATE "ResultsSchema"."{table}" t
                SET national_rank = f.national_rank,
                    lsc_rank      = f.lsc_rank,
                    club_rank     = f.club_rank
                FROM final_ranks f
                WHERE t."UsasSwimTimeKey" = f."UsasSwimTimeKey" """
                cur.execute(query)

                query = f"""UPDATE "ResultsSchema"."SwimmerIDs" AS i
                        SET "Sex" = r."Sex"
                        FROM "ResultsSchema"."{table}" AS r
                        WHERE i."PersonKey" = r."PersonKey" """
                cur.execute(query)

        conn.commit()

if __name__ == "__main__":
    send_rankings_query()