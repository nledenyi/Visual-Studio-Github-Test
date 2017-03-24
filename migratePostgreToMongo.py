import json
import psycopg2
from psycopg2.extras import *
from config import config
from pymongo import MongoClient

try:
    params = config()
    conn = psycopg2.connect(**params)

    cur = conn.cursor(cursor_factory=RealDictCursor)

    table_name = "nobel_country"
    #table_name = "nobel_prize"
    #table_name = "nobel_laureate"

    cur.execute("SELECT * FROM " + table_name)

    data = json.loads(json.dumps(cur.fetchall()))

    client = MongoClient()

    db = client["sandbox1"]
    collection = db[table_name]
    print(collection.count())

    collection.insert_many(data)

    print(collection.count())

    # cursor = db["nobel_country"].find()
    # for doc in cursor:
    #     print (doc)

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed.')