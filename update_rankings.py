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
    dbname, port, password, host = get_credentials()

    table_names = [
        '50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results',
        '100_FR_LCM_results', '200_FR_SCY_results', '200_FR_LCM_results',
        '400_FR_LCM_results', '500_FR_SCY_results', '800_FR_LCM_results',
        '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results',
        '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results',
        '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results',
        '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results',
        '200_IM_LCM_results', '400_IM_LCM_results'
    ]

    with psycopg.connect(f"dbname={dbname} port={port} user=postgres host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            # 🔥 Batch job optimizations
            cur.execute("SET synchronous_commit = OFF;")

            for table in table_names:
                table_id = sql.Identifier("ResultsSchema", table)
                cur.execute("DROP TABLE IF EXISTS tmp_best_times;")
                cur.execute("DROP TABLE IF EXISTS tmp_ranked;")
                # 1️⃣ Best times for season only
                cur.execute(sql.SQL("""
                    CREATE TEMP TABLE tmp_best_times ON COMMIT DROP AS
                    SELECT
                        "PersonKey",
                        "Sex",
                        "AgeGroup",
                        "LSC",
                        "Team",
                        MIN("SwimTime") AS best_time
                    FROM {}
                    WHERE "SwimDate" >= %s
                      AND "SwimDate" <  %s
                    GROUP BY
                        "PersonKey","Sex","AgeGroup","LSC","Team";
                """).format(table_id), (season_start, season_end))

                # 2️⃣ Rank once
                cur.execute("""
                    CREATE TEMP TABLE tmp_ranked ON COMMIT DROP AS
                    SELECT *,
                        RANK() OVER (
                            PARTITION BY "Sex","AgeGroup"
                            ORDER BY best_time
                        ) AS national_rank,
                        RANK() OVER (
                            PARTITION BY "Sex","AgeGroup","LSC"
                            ORDER BY best_time
                        ) AS lsc_rank,
                        RANK() OVER (
                            PARTITION BY "Sex","AgeGroup","Team"
                            ORDER BY best_time
                        ) AS team_rank
                    FROM tmp_best_times;
                """)

                # 3️⃣ Update ONLY best-time rows
                cur.execute(sql.SQL("""
                    UPDATE {}
                    SET
                        national_rank = r.national_rank,
                        lsc_rank      = r.lsc_rank,
                        team_rank     = r.team_rank
                    FROM tmp_ranked r
                    WHERE {}."PersonKey" = r."PersonKey"
                      AND {}."SwimTime"  = r.best_time
                      AND {}."SwimDate" >= %s
                      AND {}."SwimDate" <  %s;
                """).format(
                    table_id,
                    table_id,
                    table_id,
                    table_id,
                    table_id
                ), (season_start, season_end))

            # 4️⃣ Update SwimmerIDs ONCE
            cur.execute("""
                UPDATE "ResultsSchema"."SwimmerIDs" i
                SET "Sex" = r."Sex"
                FROM (
                    SELECT DISTINCT "PersonKey", "Sex"
                    FROM "ResultsSchema"."50_FR_SCY_results"
                ) r
                WHERE i."PersonKey" = r."PersonKey";
            """)

        conn.commit()

if __name__ == "__main__":
    send_season_ranking_query("2025-09-01", "2026-08-31", "2025-2026")