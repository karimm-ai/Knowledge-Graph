from falkordb import FalkorDB

db = FalkorDB(host='localhost', port=6379)
g = db.select_graph("book_graph")

res = g.query("""MATCH (n:Book)-[r]-(other)
                        RETURN DISTINCT type(r) AS relationship_type, labels(other) AS other_node_labels
                    """)
for row in res.result_set:
    print(row[1][0])