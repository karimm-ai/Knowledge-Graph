from falkordb import FalkorDB
import json

class Schema():
    #def __init__():
        #self.schema_client = QuerySystem.db
        #self.g = QuerySystem.db.select_graph(schema_name)
     #   return
        


    def create_schema(self, schema_json_file):
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


    def add_schema_node(self, node_name, properties=None):
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


    def update_schema_node(self, node_name, new_properties, conditional_properties=None):
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




    def add_schema_relationship(self, from_label, rel_type, to_label):       
        q = f"""
        MATCH (from:{from_label}), (to:{to_label})
        CREATE (from)-[:{rel_type}]->(to)
        """
        self.g.query(q.strip())
    

    def update_schema_relationship(self, node1, node2, new_rel, node1_attr=None, node2_attr=None, new_rel_attr=None):
        q = f"""MATCH (n1:{node1}"""
        if not node1_attr == None:
            n_attr = []
            for attr, val in node1_attr.items():
                n_attr.append(f"{attr}: '{val}'")
            
            n_attr = ", ".join(n_attr)
            q = ''.join((q, f" {{{n_attr}}})"))
        
        else:
            q = ''.join((q, ")"))

        q = ''.join((q, f"-[r]->(n2:{node2}"))
        if not node2_attr == None:
            n_attr = []
            for attr, val in node2_attr.items():
                n_attr.append(f"{attr}: '{val}'")
            
            n_attr = ", ".join(n_attr)
            q = ''.join((q, f" {{{n_attr}}})\n"))
        

        else:
            q = ''.join((q, ")\n"))


        rel_create_query = f"""DELETE r\nCREATE (n1)-[new_r:{new_rel}]->(n2)"""

        if not new_rel_attr==None:
            rel_attr = []
            for attr, val in new_rel_attr.items():
                rel_attr.append(f"new_r.{attr}= '{val}'")
            rel_attr = ", ".join(rel_attr)
            
            rel_create_query = ''.join((rel_create_query, "\n", f"SET {rel_attr}"))
        
        final_query = ''.join((q, rel_create_query))
        print(final_query)
        self.g.query(f"{final_query.strip()} RETURN n1, n2, new_r")




    def add_node_constraints(self, c_type, node_name, attributes):

        if c_type.upper() == "UNIQUE":
            self.g.query(f"CREATE INDEX FOR (n:{node_name}) ON (n.{attributes})")

        self.g._create_constraint(f'{c_type.upper()}','NODE', f'{node_name}', attributes)
  

    def add_relationship_constraints(self, relationship_name, type, attributes):###############################################################TEST#######
            self.schema_client.create_constraint(self.g, f'{type.upper()}','RELATIONSHIP', f'{relationship_name}', f'{attributes}')

    



class KnowledgeGraph():
    #def __init__(self, QuerySystem, kg_name):
    #    self.g = QuerySystem.db.select_graph(kg_name)


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
    



    def create_kg_node(self, node_name, properties=None, check_schema=None):
        output = False

        if not check_schema == None:
            output = self.check_schema(node_name, properties, check_schema)

        else:
            output = True


        if output:
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


    def update_kg_node(self, node_name, new_properties, conditional_properties=None):
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




    def add_kg_relationship(self, from_label, from_prop_key, from_prop_val, rel_type, to_label, to_prop_key, to_prop_val):

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
    

    def update_kg_relationship():
        pass





class QuerySystem(Schema, KnowledgeGraph):
    def __init__(self, db_ip_address, db_port, graph_name=None):
        
        self.db_ip_address = db_ip_address
        self.db_port = db_port
        self.graph_name = graph_name
        self.db = FalkorDB(host=self.db_ip_address, port=self.db_port)
        self.g = self.db.select_graph(self.graph_name)
        print(self.g.name)

        #if not graph_name==None:
         #   if graph_name.endswith("schema"):
          #      self.schema = Schema(self, graph_name)

           # else:
            #    self.kg = KnowledgeGraph(self, graph_name)



    def find_relationship():
        pass    

    

    def find_node(self, node_name, properties=None, make_exist=False):
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
  


    def check_schema(self, node_name, properties, check_schema):
        self.sg = self.db.select_graph(check_schema)
        res = self.sg.query("MATCH (n) RETURN DISTINCT labels(n)[0] AS nodes")
        schema_labels = res.result_set
        label_exist_check = False
        data_types_check = False
        failed_operation = False
        found_label = True
        correct_data = True

        for element in schema_labels:
            if node_name == element[0]:
                label_exist_check = True
                break
        
        if label_exist_check:
            res = self.sg.query(f"MATCH (n:{node_name}) RETURN properties(n)")
            props = res.result_set[0]
            #{'name': 'Ahmed', 'id': '100', 'age': 30}
            for attr, value in properties.items():
                if found_label and correct_data:
                    found_label = False
                    correct_data = False                 
                                            

                for props_key, props_val in dict(props[0]).items():
                    if attr == props_key:
                        
                        found_label = True
                        value_type = type(value).__name__
                        
                        if value_type == 'str':
                            value_type = 'string'
                        
                        if value_type == 'int':
                            value_type = 'integer'

                        if value_type == props_val:
                            correct_data = True
                            break
                        
                        if not value_type == props_val:
                            correct_data = False
                            print("Your attribute has incorrect data type")
                            failed_operation = True
                            break
                
                if not found_label:
                        print("You have entered a wrong attribute name")
                        failed_operation = True
                        break
            
        if not failed_operation:
            data_types_check = True

        return label_exist_check and data_types_check and self.check_schema_constraints(node_name, properties, check_schema)



    def check_schema_constraints(self, node_name, properties, check_schema):
        self.sg = self.db.select_graph(check_schema)
        res = self.sg.ro_query("call db.constraints()")
        constraints = []
        pass_ = True
        found_key = False

        for row in res.result_set:
            if row[1] == node_name:
                constraints.append(row)

        for constraint in constraints:
            if not pass_:
                break
            
            if found_key:
                found_key = False

            for key, value in properties.items():
                if not pass_:
                    break

                if found_key:
                    break

                if key in constraint[2]: ##################WHAT IF MULTIPLE KEYS IN CONSTRAINT#####################
                    found_key = True
 
                    if constraint[0].upper() == "UNIQUE":
                        res = self.g.query((f"MATCH (n:{node_name}) RETURN properties(n)"))
                        for element in res.result_set:
                            if not pass_:
                                break

                            for e_key, e_value in dict(element[0]).items():
                                if e_key == key:
                                    if value == e_value:
                                        pass_=False
                                        break
            
            if not found_key:
                print(f"{constraint[2]} is missing from your input.")
                pass_= False
                return pass_


            

        if not pass_:
            print('Value already exists')            
        
        return pass_
            
