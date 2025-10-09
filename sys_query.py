from falkordb import FalkorDB
import json

class Schema():

#CREATE

    def create_schema(self, schema_json_file):

        if self.is_schema_graph():
            
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
                self.add_schema_node(key, value)

            create_schema_relationships(schema)

        else:
            print("Invalid Graph Selected. Please select a schema graph!")



#ADD

    def add_schema_node(self, node_name, properties=None):
        if self.find_node(node_name, properties):
            print(f"Node already exists in the Schema!")
        
        else:
            q = f"""CREATE (n:{node_name.replace('"', '')} 
            """
            
            if not properties == None:
                node_properties = []
                for key, value in properties.items():
                    node_properties.append(f"{key}: '{value}'")

                node_properties = ", ".join(node_properties)
            
                q = ''.join((q, f"{{{node_properties}}})"))

            try:
                res = self.g.query(q)
                print(f"Successfully added {res.nodes_created} node(s) to the schema")

            except Exception as e:
                print(f"ERROR Creating Node: {e}")




    def add_schema_relationship(self, from_label, rel_type, to_label, rel_attributes=None):
        if self.find_node(from_label) and self.find_node(to_label):
            if not rel_attributes == None:
                props = ", ".join([f"{k}: '{v}'" for k, v in props.items()])
                q = f"""
                MATCH (from:{from_label}), (to:{to_label})
                CREATE (from)-[:{rel_type} {{{props}}}]->(to)
                """

            else: 
                q = f"""
                MATCH (from:{from_label}), (to:{to_label})
                CREATE (from)-[:{rel_type}]->(to)
                """

            self.g.query(q.strip())

        else:
            print("One of the label nodes does not exist in the schema!")
    



    def add_node_constraints(self, c_type, node_name, attributes):
        if self.find_node(node_name):
            node_attr = self.find_node_attr(node_name)
            if attributes in node_attr:
                if c_type.upper() == "UNIQUE":
                    self.g.query(f"CREATE INDEX FOR (n:{node_name}) ON (n.{attributes})")

                self.g._create_constraint(f'{c_type.upper()}','NODE', f'{node_name}', attributes)
            
            else:
                print(f"{attributes} is not an attribute for node {node_name} !")

        else:
            print(f"Node {node_name} does not exist in the schema!")




    def add_relationship_constraints(self, c_type, rel_type, attributes):#####TEST#######
            if self.find_relationship(rel_type):
                if c_type.upper() == "UNIQUE":
                    self.g.query(f"CREATE INDEX FOR (r:{rel_type}) ON (r.{attributes})")

                self.g._create_constraint(self.g.name, f'{c_type.upper()}','RELATIONSHIP', f'{rel_type}', f'{attributes}')

    


#UPDATE

    def update_schema_node(self, node_name, new_properties, conditional_properties=None):
        if self.find_node(node_name, conditional_properties):
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
        
        else:
            print(f"Node {node_name} with the following conditions {conditional_properties} does not exist in DB!")




    def update_schema_relationship(self, node1, node2, new_rel, node1_attr=None, node2_attr=None, new_rel_attr=None):
        if self.find_node(node1, node1_attr) and self.find_node(node2, node2_attr) and self.is_rel_exist(node1, node1_attr, node2, node2_attr):
            qinit = "MATCH "
            q1 = self.match_statement(node1, node1_attr)
            qmid = f"-[r]->"
            q2 = self.match_statement(node2, node2_attr)
            q = ''.join((qinit, q1, qmid, q2, "\n"))

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

        else:
            print("Nodes or relationships nonexistent!")




class KnowledgeGraph():

#CREATE

    def create_knowledge_graph(self, kg_name, kg_schema_json):
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
    


#ADD

    def add_kg_node(self, node_name, properties=None, check_schema=None):
        if self.find_node(node_name, properties):
            print(f"Node {node_name} with attributes {properties} already exists in the graph!")

        else:
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
                    q = ''.join((q, f"{{{node_properties}}})")) ##TEST
            
                try:
                    res = self.g.query(q)
                    print(f"Sucessfully added {res.nodes_created} nodes to the graph!")
                except Exception as e:
                    print(f"ERROR Creating Node: {e}")



    def add_kg_relationship(self, from_label, from_label_props, rel_type, to_label, to_label_props, check_schema=None):
        if self.find_node(from_label, from_label_props) and self.find_node(to_label, to_label_props):
            output = True
            if not check_schema == None:
                self.sg = self.db.select_graph(check_schema)
                if not self.is_rel_exist(self.sg, from_label, from_label_props):
                    output = False
                    print("NO RELATIONSHIP between the 2 nodes found in the schema graph!")
            
            
            if output:
                def format_val(val):
                    if isinstance(val, str):
                        val = val.replace("'", "\\'")
                        return f"'{val}'"
                    else:
                        return str(val)
                
                q = f"""
                MATCH (from:{from_label} {{{from_label_props}}}), (to:{to_label} {{{to_label_props}}})
                CREATE (from)-[:{rel_type}]->(to)
                """
                try:
                    self.g.query(q.strip())
                    print(f"Successfully created an {rel_type} edge between {from_label} {{{from_label_props}}} and {to_label} {{{to_label_props}}}")
                except Exception as e:
                    print(f"Failed: {e}")


        else:
            ("At least one of the nodes in nonexistent!")       



#UPDATE

    def update_kg_node(self, node_name, new_properties, conditional_properties=None, check_schema=None):
        if self.find_node(node_name, conditional_properties):
            conditional_properties = conditional_properties or {}
            full_props =  {**conditional_properties, **new_properties}
            if self.check_schema(node_name, full_props, check_schema, let_pass=conditional_properties):
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
        
        else:
            print(f"Node {node_name} with properties {conditional_properties} does not exist in the graph!")
 


    def update_kg_relationship(self, node1, node2, new_rel, node1_attr=None, node2_attr=None, new_rel_attr=None, check_schema=None):
        if self.find_node(node1, node1_attr) and self.find_node(node2, node2_attr) and self.is_rel_exist(node1, node1_attr, node2, node2_attr):
            qinit = "MATCH "
            q1 = self.match_statement(node1, node1_attr)
            qmid = f"-[r]->"
            q2 = self.match_statement(node2, node2_attr)
            q = ''.join((qinit, q1, qmid, q2, "\n"))

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

        else:
            print("Nodes or relationships nonexistent!") 





class QuerySystem(Schema, KnowledgeGraph):
    def __init__(self, db_ip_address, db_port, graph_name=None):
        
        self.db_ip_address = db_ip_address
        self.db_port = db_port
        self.graph_name = graph_name
        self.db = FalkorDB(host=self.db_ip_address, port=self.db_port)
        self.g = self.db.select_graph(self.graph_name)
        print(self.g.name)


#FIND

    def find_relationship(self, rel_type):
        res = self.g.query("MATCH (n)-[r]->(m) RETURN type(r)")
        for row in res.result_set:
            if row[0] == rel_type:
                return True

        return False    

    


    def find_node(self, node_name, properties=None, make_exist=False, check_schema=None):
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
    
        try:
            res = self.g.query(q)
            if len(res.result_set) > 0:
                print(f"Found {len(res._result_set)} Nodes!")
                for row in res.result_set:
                    print(row[0])
                return True

            else:
                if make_exist:
                    if self.g.name.endswith("_schema"):
                        self.add_schema_node(node_name, properties)

                    else:
                        self.add_kg_node(node_name, properties=properties, check_schema=check_schema)
                else:
                    print("No Nodes Found!")
                    return False
        except Exception as e:
            print(f"ERROR querying: {e}")
  



    def find_node_attr(self, node_name):
        res = self.g.query(f"MATCH (n:{node_name}) RETURN properties(n)")
        all_attr = []
        for element in res.result_set:
            props = dict(element[0]).keys()
            for key in props:
                all_attr.append(key)

        return all_attr




    def find_rel_attr(self, rel_type):
        pass




#CHECK

    def check_schema(self, node_name, properties, check_schema, let_pass=None):
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
                            print(value_type, props_val)
                            correct_data = False
                            print("Your attribute has incorrect data type")
                            failed_operation = True
                            break
                
                if not found_label:
                        print(f"Attribute name does not exist for node {node_name}")
                        failed_operation = True
                        break
            
        if not failed_operation:
            data_types_check = True

        return label_exist_check and data_types_check and self.check_schema_constraints(node_name, properties, check_schema, let_pass)




    def check_schema_constraints(self, node_name, properties, check_schema, let_pass=None):
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
                                if e_key == key and value == e_value:
                                        for k, v in let_pass.items():
                                            if e_key == k and v == value:
                                                passed_key = True
                                                break
                                        
                                        if not passed_key:
                                            pass_=False
                                            break
                                        else:
                                            break
                    
                    else:
                        break
            
            if not found_key:
                print(f"{constraint[2]} is missing from your input.")
                pass_= False
                return pass_


            

        if not pass_:
            print('Value already exists')            
        
        return pass_
            



#OTHERS

    def is_schema_graph(self):
        if self.g.name.endswith("_schema"):
            return True
        
        return False
    
    


    def is_rel_exist(self, graph, node1, node2, node1_attr=None, node2_attr=None):
        qinit = "MATCH "
        q1 = self.match_statement(node1, node1_attr)
        qmid = f"-[r]->"
        q2 = self.match_statement(node2, node2_attr)
        q = ''.join((qinit, q1, qmid, q2, ' RETURN type(r)'))
        res = graph.query(q)
        if len(res.result_set) > 0:
            return True

        return False




    def match_statement(self, node, node_attr=None):
        q = f"""({node}"""
        
        if not node_attr == None:
            n_attr = []
            for attr, val in node_attr.items():
                n_attr.append(f"{attr}: '{val}'")
            
            n_attr = ", ".join(n_attr)
            q = ''.join((q, f" {{{n_attr}}})"))
        
        else:
            q = ''.join((q, ")"))

        return q