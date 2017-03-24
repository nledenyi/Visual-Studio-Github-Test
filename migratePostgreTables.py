import subprocess
import os
from config import config

params = config()

pgSourceDb = "sandbox1"
pgDestDb   = "sandbox2" 
pgTables = ["nobel_country"] # , "nobel_prize", "nobel_laureate"
pgRuntime = "c:\\Program Files (x86)\\pgAdmin 4\\v1\\runtime\\"

for pgTable in pgTables:
    print(pgTable)
    pgDump = subprocess.Popen(
        [pgRuntime+'pg_dump.exe', '-h', params['host'], '-p', params['port'], '-d', pgSourceDb, '-U', params['user'], '-t', pgTable],
        stdout=subprocess.PIPE
    )
    pgPsql = subprocess.check_output(
        [pgRuntime+'psql.exe', '-h', params['host'], '-p', params['port'], '-d', pgDestDb, '-U', params['user']], 
        stdin=pgDump.stdout
    )
    pgDump.wait()
    print(pgTable+" done")