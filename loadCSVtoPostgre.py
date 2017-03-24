import psycopg2
import csv
from config import config

def loadData(conn,csvPath,tableName,tableColumns,csvColumns):
    cur = conn.cursor()
    
    tableValues = ("%s," * len(tableColumns))[:-1]
    #tableColumns = ','.join(tableColumns)
    tableColumnList = ""
    for column in tableColumns:
        tableColumnList += '"'+column+'",'
    tableColumnList = tableColumnList[:-1]
    
    sql = "INSERT INTO {0}({1}) VALUES({2});".format(tableName,tableColumnList,tableValues)

    with open(csvPath, newline='') as csvfile:
        datadict = csv.DictReader(csvfile)
        for row in datadict:
            values = ()
            for col in csvColumns:
                values = values + (row[col],)
            cur.execute(sql,values)
    cur.close()
    conn.commit()

try:
    params = config()
    conn = psycopg2.connect(**params)

    tableToLoad = "nobel_laureate"

    if tableToLoad == "nobel_prize":
        csvPath = tableToLoad + ".csv"
        tableColumns = ["year", "category", "nobel_id", "firstname", "surname", "motivation", "share"]
        csvColumns = ["year", "category", "id", "firstname", "surname", "motivation", "share"]
        tableName = tableToLoad
    elif tableToLoad == "nobel_laureate":
        csvPath = tableToLoad + ".csv"
        tableColumns = ["nobel_id", "firstname", "surname", "born", "died", "bornCountry", "bornCountryCode", "diedCountryCode", "diedCity", "gender", "year", "category", "share", "motivation", "name", "city", "country"]
        csvColumns = ["id", "firstname", "surname", "born", "died", "bornCountry", "bornCountryCode", "diedCountryCode", "diedCity", "gender", "year", "category", "share", "motivation", "name", "city", "country"]
        tableName = tableToLoad
    elif tableToLoad == "nobel_country":
        csvPath = tableToLoad + ".csv"
        tableColumns = ["name", "code"]
        csvColumns = tableColumns
        tableName = tableToLoad

    loadData(conn, csvPath, tableName, tableColumns, csvColumns)

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed.')


# speed up insert: http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query

