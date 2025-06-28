import psycopg 
from psycopg import sql
with psycopg.connect("dbname=SwimRank port=5462 user=postgres host='localhost' password='Annoyer9Ores!2345'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
            for db in db_table_names:
                query = sql.SQL("DELETE FROM {""}.{""}").format(
                        sql.Identifier('ResultsSchema'),
                        sql.Identifier(db),           
                    )
                """query = sql.SQL("ALTER TABLE {""}.{""} ADD sex integer").format(
                        sql.Identifier('ResultsSchema'),
                        sql.Identifier(db),           
                    )"""
                cur.execute(query)
                conn.commit()