import sqlite3
import pandas as pd
import sqlalchemy as db
from tabulate import tabulate

# Create your connection.
engine = db.create_engine('sqlite:///nyc_database.db')
connection = engine.connect()
metadata = db.MetaData()
census = db.Table('yellow_cab', metadata, autoload=True, autoload_with=engine)
# print(census.columns.keys())
query = db.select([census])
ResultProxy = connection.execute(query)
print("-" * 5)
# ResultSet = ResultProxy.fetchall()
ResultSet = []
flag = True
while flag:
    print("1" * 5)
    partial_results = ResultProxy.fetchmany(5)
    print(partial_results)
    if not partial_results:
        ResultSet += partial_results
    print("2" * 5)
ResultProxy.close()
print("-" * 5)

print(ResultSet)
df = pd.DataFrame(ResultSet)
df.columns = ResultSet[0].keys()
# print(tabulate(df, headers='keys', tablefmt='psql'))
