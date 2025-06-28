import json
import psycopg
from psycopg import sql 

def send_data(data):
    columns = ['Name', 'Club', 'LSC', 'Age', 'PersonKey']
    query = sql.SQL("INSERT INTO {""}.{""} ({}) VALUES ({})").format(
            sql.Identifier('ResultsSchema'),
            sql.Identifier('SwimmerIDs'),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(map(sql.Placeholder, columns))             
        )
    cur.executemany(query, data)
    conn.commit()

with open('ids.json', 'r') as file:
    swimmers = file.readlines()
    #just put GA swimmers in the database for now. Will have to clear later
    swimmers = [json.loads(swimmer) for swimmer in swimmers]# if swimmer[swimmer.index('LSC') + 6: swimmer.index('LSC') + 8] == 'GA']
    print(len(swimmers))
    with psycopg.connect("dbname=SwimRank port=5462 user=postgres host='localhost' password='Annoyer9Ores!2345'") as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            i = 0
            while i < len(swimmers) - 10000: 
                temp = swimmers[i:i+10000]
                print(temp)
                send_data(temp)
                i += 10000
            temp = swimmers[i:]
            send_data(temp)
