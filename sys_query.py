from falkordb import FalkorDB
import json

class QuerySystem():
    def __init__(self, db_ip_address,
                 db_port,
                 graph_name=None):
        
        self.db_ip_address = db_ip_address
        self.db_port = db_port
        self.graph_name = graph_name
        self.db = FalkorDB(self.db_ip_address, self.db_port)

        if not graph_name==None:
            self.g = self.db.select_graph(self.graph_name)




    def create_schema(self, schema_name, 
                      schema_json_file):
        self.g = self.db.select_graph(schema_name)
        
        def load_json(path):
            with open(path, "r") as f:
                return json.load(f)
            
        def create_schema_relationships(data):
            for key, value in data["relationships"].items():
                src = value["from"]
                dst = value["to"]

                q = f"""MATCH (s:{src}), (d:{dst})
                    CREATE (s)-[:{key}]->(d)
                """
                self.g.query(q.strip())
            
        schema = load_json(schema_json_file)
        

        for key, value in schema["nodes"].items():
            key_props = ", ".join([f"{prop}: '{value}'" for prop, value in value.items()])

            q = f"""
                CREATE (:{key} {{{key_props}}})
            """
            self.g.query(q)

        create_schema_relationships(schema)
        self.schema = Schema(self, schema_name)


    

    def find_node(self, node_name,
            properties=None, make_exist=False):
        q = f"""MATCH (n:{node_name.replace('"', '')})"""
        query_conditions = []
        
        if not properties == None:
            q = ''.join((q, "\n", "WHERE "))
            
            for key, value in properties.items():
                query_conditions.append(f"n.{str(key)}='{value}' ")
            query_conditions = "AND ".join(query_conditions)
           
            q = ''.join((q, query_conditions, "\nRETURN n"))
        
        else:
            q = ''.join((q, "\nRETURN n"))
    
        print(q)
        try:
            res = self.g.query(q)
            if len(res.result_set) > 0:
                print(f"Found {len(res._result_set)} Nodes!")
                for row in res.result_set:
                    print(row[0])

            else:
                print("No Nodes Found!")
                if make_exist:
                    self.create_node(node_name, properties=properties)
        except:
            print("ERROR querying")

    
    

    def create_node(self, node_name, properties=None):
        q = f"""CREATE (n:{node_name.replace('"', '')} 
         """
        
        

        if not properties == None:
            node_properties = []
            for key, value in properties.items():
                node_properties.append(f"{key}: '{value}'")

            node_properties = ", ".join(node_properties)
            print(node_properties)
           
            q = ''.join((q, f"{{{node_properties}}})")) ##TEST
    
        print(q)
        try:
            res = self.g.query(q)
            print(res.nodes_created)
        except:
            print("ERROR Creating Node:")

    


    def build_knowledge_graph(self, kg_name, kg_schema_json):
        ontology = self.load_json(kg_schema_json)
        queries = []

        for _, data in ontology.items():
            if isinstance(data, dict):
                for obj, props in data.items():
                    props = ", ".join([f"{k}: '{v}'" for k, v in props.items()])
                    queries.append(f"CREATE (:{obj} {{{props}}});")

        for query in queries:
            print(query)

        # Create relationships
        for class_, data in ontology.items():
            if isinstance(data, dict):
                q= f"""MATCH (n:{class_})-[r]->(other)
                        RETURN r, other
                    """
                 
                res = self.g.query(q)
                rel_with_classes = {}

                for row in res.result_set:
                    rel_with_classes[row[1]] = row[0]
                    

                for cls, rel in rel_with_classes.items():
                    for class_, data in ontology.items():
                        if cls == class_:
                            break
                    #for obj, props in data.items():    
                    #    rel_type = rel
                    #    src = obj
                        
                    #    dst = 

                q = f"""
            MATCH (a {{id: '{src}'}}), (b {{id: '{dst}'}})
            CREATE (a)-[:{rel_type}]->(b);
            """
            queries.append(q.strip())
        self.kg = KnowledgeGraph(self, kg_name)

        return queries
    


            
    def update_node(self, node_name, new_properties, conditional_properties=None):
        q = f"""MATCH (n:{node_name})
            """
        query_conditions = []
        new_query_properties = []

        if not conditional_properties==None:
            q = ''.join((q, "WHERE "))
            
            for key, value in conditional_properties.items():
                query_conditions.append(f"n.{str(key)}='{value}' ")

            query_conditions = "AND ".join(query_conditions)
           
            q = ''.join((q, query_conditions, "\nSET "))
        
        else:
            q = ''.join((q, "\nSET "))
        

        for key, value in new_properties.items():
            new_query_properties.append(f"n.{key}='{value}' ")
        
        new_query_properties = ", ".join(new_query_properties)
        
        q = ''.join((q, new_query_properties))
        print(q)
        try:
            self.g.query(q)
            print("Node Successfully updated!")

        except Exception as e:
            print(f"ERROR UPDATING {e}")






class Schema(QuerySystem):
    def __init__(self,  QuerySystem, schema_name):
        self.g = QuerySystem.db.select_graph(schema_name)



    def add_relationship(self, from_label,
                              rel_type,
                              to_label):
    
        def format_val(val):
            if isinstance(val, str):
                val = val.replace("'", "\\'")
                return f"'{val}'"
            else:
                return str(val)
        
        q = f"""
        MATCH (from:{from_label}), (to:{to_label})
        CREATE (from)-[:{rel_type}]->(to)
        """
        self.g.query(q.strip())
    




class KnowledgeGraph(QuerySystem):
    def __init__(self, QuerySystem, kg_name):
        self.g = QuerySystem.db.select_graph(kg_name)



    def create_relationship(self, from_label,
                            from_prop_key,
                            from_prop_val,
                            rel_type,
                            to_label,
                            to_prop_key,
                            to_prop_val):

        def format_val(val):
            if isinstance(val, str):
                val = val.replace("'", "\\'")
                return f"'{val}'"
            else:
                return str(val)
        
        q = f"""
        MATCH (from:{from_label} {{{from_prop_key}: {format_val(from_prop_val)}}}), (to:{to_label} {{{to_prop_key}: {format_val(to_prop_val)}}})
        CREATE (from)-[:{rel_type}]->(to)

        """
        return q.strip()