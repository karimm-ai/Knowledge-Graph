from falkordb import FalkorDB

db = FalkorDB(host='localhost', port=6379)
g = db.select_graph("book_graph")

res = g.query("MATCH (n) RETURN n")
for row in res.result_set:
    print(row[0])

