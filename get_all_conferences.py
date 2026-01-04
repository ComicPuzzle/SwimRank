import pandas as pd
import psycopg
from psycopg import sql 
from get_credentials import get_credentials

def get_all_conferences():
    dbname, port, password, host, _ = get_credentials()
    with psycopg.connect(f"dbname={dbname} port={port} user=swimrank_write host='{host}' password='{password}'") as conn:
        with conn.cursor() as cur:
            query = """SELECT "ConferenceName" FROM "ResultsSchema"."DivI_Male"
                        UNION 
                        SELECT "ConferenceName" FROM "ResultsSchema"."DivI_Female"
                    """
            cur.execute(query)
            divI = [x[0] for x in cur.fetchall()]
            with open('DivI_conferences.txt', 'w') as f:
                for line in divI:
                    f.write(f"{line}\n")

            query = """SELECT "ConferenceName" FROM "ResultsSchema"."DivII_Male"
                        UNION 
                        SELECT "ConferenceName" FROM "ResultsSchema"."DivII_Female"
                    """
            divII = cur.execute(query)
            divII = [x[0] for x in cur.fetchall()]
            with open('DivII_conferences.txt', 'w') as f:
                for line in divII:
                    f.write(f"{line}\n")

            query = """SELECT "ConferenceName" FROM "ResultsSchema"."DivIII_Male"
                        UNION 
                    SELECT "ConferenceName" FROM "ResultsSchema"."DivIII_Female"
                    """
            divIII = cur.execute(query)
            divIII = [x[0] for x in cur.fetchall()]
            with open('DivIII_conferences.txt', 'w') as f:
                for line in divIII:
                    f.write(f"{line}\n")

if __name__ == "__main__":
    get_all_conferences()