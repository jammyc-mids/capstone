import psycopg2
import json
import re

class pgDriver:
    def __init__(self, autocommit=False):
        self.server = "ec2-34-221-49-209.us-west-2.compute.amazonaws.com"
        self.database = "energy_transformers"
        self.username = "capstone"
        self.password = "Mids2024!"
        self.dbh = self._connect(self.database, self.server, self.username, self.password)
        self.cur = self.dbh.cursor()
        self.dbh.autocommit = autocommit
 
    def _connect(self, dbname, server, user, passwd): 
        connectString = "dbname=" + dbname + " host=" + server + " user=" + user + " password=" + passwd
        try:
            return psycopg2.connect(connectString)
        except:
            print(f"Connecting PostgreSQL database failed: {server}") 
            return None

    # CRUD operations
    def _read(self, query):
        self.cur.execute(query)
        result = self.cur.fetchall()
        return [r for r in result]

    def _update(self, query):
        self.cur.execute(query)
        if self.dbh.autocommit == False:
            self.dbh.commit()
        return self.cur.rowcount
