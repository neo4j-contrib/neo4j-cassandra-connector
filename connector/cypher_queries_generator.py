import re
import os
from collections import namedtuple
from node_structure import NodeStructure

class CypherQueriesGenerator(object):
  def __init__(self, keyspace):
    self.keyspace = keyspace
    self.node = None
    self.nodes = []

  def generate(self):
    yaml_file = open(os.path.join(os.getcwd(),'schema.yaml'), 'r')
    for line in yaml_file:
      create_table_match = re.match("CREATE TABLE (.*)", line)
      if (create_table_match):
        if(self.node):
          self.nodes.append(self.node)
        self.node = NodeStructure()
        self.format_node(create_table_match)
        continue
      pk_match = re.match("\s*PRIMARY KEY \(.*", line)
      if(pk_match):
        continue
      self.collect_filled_parameters(line)
    return self.nodes

  def build_queries(self, nodes, tables, result_csvs):
    cypher_file = open('cypher_', 'w+')
    columns = self.analyse_csv(result_csvs)
    nodes_structure = self.analyse_node(tables, nodes)
    # builds a tuple with all needed information to build a Cypher query
    Builder = namedtuple('Builder', 'node_name csv_file column_size node_structure')
    info_tuple = zip(tables, result_csvs, columns, nodes_structure)
    for element in info_tuple:
      element = Builder(element[0], element[1], element[2], element[3])
      params_hash = self.build_params_hash(element.node_structure, element.column_size)
      if(element[3].relationships):
        relationships_query = self.build_relationships(element.node_structure)
        print(relationships_query)
      #AS LINE MATCH (b:playlist:artist) WHERE b.artist=line[1] 
      query = "LOAD CSV FROM {csv_file} AS LINE CREATE (n{label} {params} );\n".format(csv_file=element.csv_file, label=element.node_structure.label, params=str(params_hash))
      cypher_file.write(query)
    
  def build_relationships(self, node):
    idx = 0
    key = ""
    for rel in node.relationships.keys():
      idx = node.properties.keys().index(rel)
      key = rel
    query = "AS LINE MATCH (n{label} WHERE n. {key}=line[{idx}] ".format(label=node.label, key=key, idx=idx)
    return query

  def analyse_node(self, tables, nodes):
    nodes_structure = []
    for idx, unique_name in enumerate(tables):
      node = self.get_node(unique_name, nodes)
      nodes_structure.append(node)
    return nodes_structure

  def build_params_hash(self, node, size):
    params = {}
    for i in range(size):
      params.update({node.properties.keys()[i]: "line["+ str(i) +"]"})
    return params


  def analyse_csv(self, result_csvs):
    columns = []
    for csv in result_csvs:
      f = open(csv, 'r')
      column = len(f.readline().split(","))
      columns.append(column)
    return columns
        
  def get_node(self, unique_name, nodes):
    for node in nodes:
      if(node.unique_name == unique_name):
        return node 

  def format_node(self, create_table_match):
    node = (str(create_table_match.group(1)))[:-1]
    node = node.replace(".", ":")
    table_id = node.split(":")[1]
    node =  ":" + node
    self.node.unique_name = table_id
    self.node.label = node 

  def collect_filled_parameters(self, line):
    filled_parameter = re.match(".*\{(.*)\}.*", line)
    if(filled_parameter):
      data = (line.lstrip()).split(":")[0]
      if(filled_parameter.group(1) == "r"):
        self.node.relationships.update({data.split(" ")[0]:data.split(" ")[1]})
      self.node.properties.update({data.split(" ")[0]:data.split(" ")[1]})

