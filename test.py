from falkordb import FalkorDB

db = FalkorDB(host='localhost', port=6379)
g = db.select_graph("book_graph")

res = g.query("MATCH (n) RETURN n")
x=0
for row in res.result_set:
    x+=1
    print(row[0])

print(f"Num of nodes is {x}")
