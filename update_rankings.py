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

def send_season_ranking_query(season_start, season_end, season):
    dbname, port, password, host, _ = get_credentials()
    table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results',
        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results',
        '100_BK_LCM_results', '200_BK_LCM_results', '50_FL_SCY_results', '100_FL_SCY_results',
        '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results', 
        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', 
        '100_BR_LCM_results', '200_BR_LCM_results', '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
    ]
    with psycopg.connect(f"dbname={dbname} port={port} user=swimrank_write host='{host}' password='{password}'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            for table in table_names:
                query = f"""
                            WITH best_times AS (
                            SELECT "UsasSwimTimeKey", "PersonKey", "Sex", "AgeGroup", "LSC", "Team", "SwimTime" as best_time
                            FROM (
                            SELECT "UsasSwimTimeKey","PersonKey", "Sex", "AgeGroup", "LSC", "Team", "SwimTime",
                            ROW_NUMBER() OVER (PARTITION BY "PersonKey", "Sex", "AgeGroup", "LSC", "Team" ORDER BY "SwimTime" ASC, "SwimDate" DESC) as rn
                            FROM "ResultsSchema"."{table}"
                            WHERE "SwimDate" >= '{season_start}' AND "SwimDate" < '{season_end}' 
                            ) as ranked
                            WHERE
                            rn = 1
                            ),

                            ranked AS (
                            SELECT
                            b.*,
                            RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup"
                            ORDER BY b.best_time
                            ) AS national_rank,

                            RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup", b."LSC"
                            ORDER BY b.best_time
                            ) AS lsc_rank,

                            RANK() OVER (
                            PARTITION BY b."Sex", b."AgeGroup", b."Team"
                            ORDER BY b.best_time
                            ) AS team_rank
                            FROM best_times b
                            ),

                            final_ranks AS (
                            SELECT
                            s."UsasSwimTimeKey",
                            CASE WHEN s.best_time = r.best_time THEN r.national_rank ELSE -1 END AS national_rank,
                            CASE WHEN s.best_time = r.best_time THEN r.lsc_rank ELSE -1 END AS lsc_rank,
                            CASE WHEN s.best_time = r.best_time THEN r.team_rank ELSE -1 END AS team_rank
                            FROM best_times s
                            LEFT JOIN ranked r
                            ON s."PersonKey" = r."PersonKey"
                            AND s."Sex" = r."Sex"
                            AND s."AgeGroup" = r."AgeGroup"
                            AND s."LSC" = r."LSC"
                            AND s."Team" = r."Team"
                            )

                            UPDATE "ResultsSchema"."{table}" t
                            SET national_rank = f.national_rank,
                            lsc_rank = f.lsc_rank,
                            team_rank = f.team_rank
                            FROM final_ranks f
                            WHERE t."UsasSwimTimeKey" = f."UsasSwimTimeKey" """
                cur.execute(query)

                query = f"""UPDATE "ResultsSchema"."SwimmerIDs" AS i
                        SET "Sex" = r."Sex"
                        FROM "ResultsSchema"."{table}" AS r
                        WHERE i."PersonKey" = r."PersonKey" """
                cur.execute(query)

        conn.commit()
