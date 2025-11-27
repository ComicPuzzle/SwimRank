import psycopg
from psycopg import sql
from get_credentials import get_credentials

dbname, port, password, host = get_credentials()

db_table_names = [
    '50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
    '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
    '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
    '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results',
    '200_BK_LCM_results', '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results',
    '100_FL_LCM_results', '200_FL_LCM_results', '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results',
    '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results', '100_IM_SCY_results', '200_IM_SCY_results',
    '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
]

db_columns = [
    "Name", "Sex", "Age", "AgeGroup", "Event", "Place", "Session", "Points", "SwimDate", "LSC",
    "Team", "Meet", "SwimTime", "Relay", "TimeStandard", "MeetKey", "UsasSwimTimeKey",
    "PersonKey", "SwimEventKey", "national_rank", "lsc_rank", "club_rank"
]

formats = [
    "text", "integer", "integer", "text", "text", "integer", "text", "integer",
    "timestamp without time zone", "text", "text", "text", "interval", "integer",
    "text", "integer", "integer", "integer", "integer", "integer", "integer", "integer"
]


with psycopg.connect(
    f"dbname={dbname} port={port} user=postgres host={host} password={password}"
) as conn:
    with conn.cursor() as cur:

        for table_name in db_table_names:
            drop_query = sql.SQL("""DROP TABLE IF EXISTS {schema}.{table} CASCADE;""").format(
                schema=sql.Identifier("ResultsSchema"),
                table=sql.Identifier(table_name)
            )
            cur.execute(drop_query)

            col_defs = [
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(fmt))
                for col, fmt in zip(db_columns, formats)
            ]

            create_query = sql.SQL("""
                CREATE TABLE {schema}.{table} ({columns}, CONSTRAINT {pkey} PRIMARY KEY ("UsasSwimTimeKey"));""").format(
                schema=sql.Identifier("ResultsSchema"),
                table=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(col_defs),
                pkey=sql.Identifier(f"{table_name}_pkey")
            )

            cur.execute(create_query)
            conn.commit()

print("All tables dropped and recreated successfully.")
