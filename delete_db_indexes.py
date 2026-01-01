import psycopg
from psycopg import sql
from get_credentials import get_credentials

SCHEMA = "ResultsSchema"

EVENT_TABLES = [
    '50_FR_SCY_results', '50_FR_LCM_results',
    '100_FR_SCY_results', '100_FR_LCM_results',
    '200_FR_SCY_results', '200_FR_LCM_results',
    '400_FR_LCM_results', '500_FR_SCY_results',
    '800_FR_LCM_results', '1000_FR_SCY_results',
    '1500_FR_LCM_results', '1650_FR_SCY_results',
    '50_BK_SCY_results', '50_BK_LCM_results',
    '100_BK_SCY_results', '100_BK_LCM_results',
    '200_BK_SCY_results', '200_BK_LCM_results',
    '50_FL_SCY_results', '50_FL_LCM_results',
    '100_FL_SCY_results', '100_FL_LCM_results',
    '200_FL_SCY_results', '200_FL_LCM_results',
    '50_BR_SCY_results', '50_BR_LCM_results',
    '100_BR_SCY_results', '100_BR_LCM_results',
    '200_BR_SCY_results', '200_BR_LCM_results',
    '100_IM_SCY_results',
    '200_IM_SCY_results', '200_IM_LCM_results',
    '400_IM_SCY_results', '400_IM_LCM_results'
]

def drop_non_pk_indexes(cur, table):
    for idx in ["Sex", "AgeGroup", "LSC", "Team", "lsc_rank", "team_rank", "national_rank"]:
        query= f""" DROP INDEX IF EXISTS "ResultsSchema"."{table}_{idx}_idx" """
        cur.execute(query)

def create_indexes(cur, table):
    # NATIONAL RANK
    cur.execute(sql.SQL("""
        CREATE INDEX {idx}
        ON {schema}.{table}
        ("AgeGroup", "Sex", "SwimDate", "national_rank")
        WHERE "national_rank" != -1;
    """).format(
        idx=sql.Identifier(f"{table}_national_rank_idx"),
        schema=sql.Identifier(SCHEMA),
        table=sql.Identifier(table)
    ))

    print(f"Created optimized indexes for {table}")

if __name__ == "__main__":
    dbname, port, password, host, _ = get_credentials()

    with psycopg.connect(f"dbname={dbname} port={port} user=swimrank_write host={host} password={password}") as conn:
        with conn.cursor() as cur:
            for table in EVENT_TABLES:
                print(f"\nProcessing table: {table}")
                drop_non_pk_indexes(cur, table)
                create_indexes(cur, table)

        conn.commit()

    print("\n✅ All indexes rebuilt successfully.")
