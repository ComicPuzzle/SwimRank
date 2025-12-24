import psycopg
from psycopg import sql
from get_credentials import get_credentials

def convert_filename(name):
        # Remove all underscores
        name = name.replace("_", "")
        # Replace the "results" part with "index"
        name = name.replace("results", "index")
        return name

if __name__ == "__main__":
    dbname, port, password, host, _ = get_credentials()

    db_table_names = ["DivI_Male",  "DivI_Female",  "DivII_Male",  "DivII_Female",  "DivIII_Male",  "DivI_Female",
        '50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results',
        '200_BK_LCM_results', '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results',
        '100_FL_LCM_results', '200_FL_LCM_results', '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results',
        '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results', '100_IM_SCY_results', '200_IM_SCY_results',
        '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
    ] #add back swimmmer ids

    db_columns = [
        "Name", "Sex", "Age", "AgeGroup", "Event", "Place", "Session", "Points", "SwimDate", "LSC",
        "Team", "Meet", "SwimTime", "Relay", "TimeStandard", "MeetKey", "UsasSwimTimeKey",
        "PersonKey", "SwimEventKey", "national_rank", "lsc_rank", "team_rank"
    ]
    formats = [
        "text", "integer", "integer", "text", "text", "integer", "text", "integer",
        "timestamp without time zone", "text", "text", "text", "interval", "integer",
        "text", "integer", "integer", "integer", "integer", "integer", "integer", "integer"
    ]
    id_columns = ["FirstName", "MiddleName", "LastName", "Team", "LSC", "Age", "Sex", "PersonKey", "Collected"]
    id_formats = ["text", "text", "text", "text", "text", "integer", "integer", "integer", "integer"]

    ncaa_columns = ["Event", "Sex", "SwimTime", "NcaaSwimTimeKey"]
    ncaa_formats = ["text", "text", "interval", "integer"]

    swimmer_ids_indexes = ["Team"]
    results_indexes = ["PersonKey", "Sex", "AgeGroup", "LSC", "Team"]
    ncaa_indexes = ["Event"]

    with psycopg.connect(f"dbname={dbname} port={port} user=swimrank_write  host={host} password={password}") as conn:
        with conn.cursor() as cur:
            for table_name in db_table_names:
                drop_query = sql.SQL("""DROP TABLE IF EXISTS {schema}.{table} CASCADE;""").format(
                    schema=sql.Identifier("ResultsSchema"),
                    table=sql.Identifier(table_name)
                )
                cur.execute(drop_query)

                if table_name == "SwimmerIDs":
                    c = id_columns
                    f = id_formats
                    k = "PersonKey"
                elif "Div" in table_name:
                    c = ncaa_columns
                    f = ncaa_formats
                    k = "NcaaSwimTimeKey"
                else:
                    c = db_columns
                    f = formats
                    k = "UsasSwimTimeKey"

                col_defs = [
                    sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(fmt))
                    for col, fmt in zip(c, f)
                ]

                create_query = sql.SQL("""
                    CREATE TABLE {schema}.{table} ({columns}, CONSTRAINT {pkey} PRIMARY KEY ({key}));""").format(
                    schema=sql.Identifier("ResultsSchema"),
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(col_defs),
                    pkey=sql.Identifier(f"{table_name}_pkey"),
                    key = sql.Identifier(k)
                )

                cur.execute(create_query)

                if table_name == "SwimmerIDs":
                    for index in swimmer_ids_indexes:
                        temp = f"""CREATE INDEX "{table_name + "_" + index + "_idx"}" ON "ResultsSchema"."{table_name}"  USING btree ("{index}") WITH (deduplicate_items=True);"""
                        temp2 = f"""CREATE INDEX idx_swimmerids_first_last ON "ResultsSchema"."SwimmerIDs" ("FirstName", "LastName");"""
                        cur.execute(temp)
                        cur.execute(temp2)
                else:
                    index_query = ""
                    if "Div" in table_name:
                        index_query = f"""CREATE INDEX "{table_name + "_event_idx"}" ON "ResultsSchema"."{table_name}"  USING btree ("Event") WITH (deduplicate_items=True);"""
                    else:
                        for index in results_indexes:
                            temp = f"""CREATE INDEX "{table_name + "_" + index + "_idx"}" ON "ResultsSchema"."{table_name}"  USING btree ("{index}") WITH (deduplicate_items=True);"""
                            index_query += temp
                    cur.execute(index_query)

                conn.commit()

    print("All tables dropped and recreated successfully.")
