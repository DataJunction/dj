import duckdb

con = duckdb.connect("default.duckdb")

with open("duckdb.sql", "r") as f:
    queries = f.read().split(";")
    
for q in queries:
    if q.strip():
        con.sql(q)
        print("Query executed successfully")
