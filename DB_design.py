import kagglehub
import pandas as pd
import json

#path = kagglehub.dataset_download("saurabhbagchi/books-dataset")
#print("Path to dataset files:", path)
json_file = "book_schema.json"

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def create_book_query(isbn, title):
    q = f"""
        CREATE (b:Book {{isbn: '{isbn.replace('"', "")}', title: '{title.replace('"', "")}'}})
    """
    return q.strip()


def create_author_query(name):
    q = f"""
    CREATE (a:Author {{name: '{name.replace('"', "")}'}})
    """
    return q.strip()


def create_publisher_query(name):
    q = f"""
    CREATE (p:Publisher {{name: '{name.replace('"', "")}'}})
    """
    return q.strip()


def create_relationship_query(from_label,
                              from_prop_key,
                              from_prop_val,
                              rel_type,
                              to_label,
                              to_prop_key,
                              to_prop_val):
    
    def format_val(val):
        if isinstance(val, str):
            return f"'{val.replace('', '')}'"
        else:
            return str(val)
    
    q = f"""
    MATCH (from:{from_label} {{{from_prop_key}: {format_val(from_prop_val)}}}), (to:{to_label} {{{to_prop_key}: {format_val(to_prop_val)}}})
    CREATE (from)-[:{rel_type}]->(to)

    """
    return q.strip()



def create_schema_relationships(data):
    for key, value in data["relationships"].items():
        src = value["from"]
        dst = value["to"]


        q = f"""
            MATCH (s:'{src}'), (d:'{dst}')
            CREATE (s)-[:'{key}']->(d)
        """
        print(q)



def create_schema(data):
    print("SCHEMA: \n")
    for key, value in data["nodes"].items():
        key_props = ", ".join([f"{prop}: '{value}'" for prop, value in value.items()])
        q = f"""
            CREATE (:'{key}' {{{key_props}}})

        """
        print(q)

    create_schema_relationships(data)
    

def print_queries(queries):
    for query in queries:
        print(query)


def construct_knowledge_graph(df):
    node_queries = []
    rel_queries = []
    for _, row in df.iterrows():
        b_isbn = row["ISBN"]
        b_title = row["Book-Title"]
        b_author = row["Book-Author"]
        b_publisher = row["Publisher"]

        node_queries.append(create_book_query(b_isbn, b_title))
        node_queries.append(create_author_query(b_author))
        node_queries.append(create_publisher_query(b_publisher))

        rel_queries.append(create_relationship_query('Book', 'isbn', b_isbn, 'AUTHORED_BY', 'Author', 'name', b_author))
        rel_queries.append(create_relationship_query('Publisher', 'name', b_publisher, 'PUBLISHED', 'Book', 'isbn', b_isbn))
    
    print("NODE QUERIES:\n")
    print_queries(node_queries)
    print("\n\n")

    print(f"RELATIONSHIP QUERIES:\n")
    print_queries(rel_queries)
    print("\n\n")


if __name__ == "__main__":
    schema = load_json(json_file)
    create_schema(schema)

    df = pd.read_csv(
    'books_mini.csv',
    encoding='cp1252',
    sep=';',
    on_bad_lines='skip',
    low_memory=False
    )   

    df = df.drop(['Image-URL-S', 'Image-URL-M', 'Image-URL-L'], axis=1)
    df = df.head(100)

    construct_knowledge_graph(df)
    print("SUCCESSFULLY CONSTRUCTED KNOWLEDGE GRAPH")

#print(create_relationship_query("Book", 'isbn', '123', 'is_Published_by', 'publisher', 'name', 'Karim'))