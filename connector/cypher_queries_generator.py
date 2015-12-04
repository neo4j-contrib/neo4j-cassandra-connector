import re
import inspect, os
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
        self.build_indexes(pk_match)
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
      params_hash_str = str(params_hash).replace("\'", "")
      path = "file://" + os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
      query = "LOAD CSV FROM \'{path}/{csv_file}\' AS line CREATE (c{label} {params} );\n".format(path=path, csv_file=element.csv_file, label=element.node_structure.label, params=params_hash_str)
      cypher_file.write(query)
      if(element.node_structure.constraints):
        constraint_query = "CREATE CONSTRAINT ON (n:{label}) ASSERT n.{constraint} IS UNIQUE;\n".format(label=self.remove_composed_label(element.node_structure.label), constraint=element.node_structure.constraints[0])
        cypher_file.write(constraint_query)
    self.build_relationships(cypher_file, info_tuple, nodes_structure)

  def build_indexes(self, pk_match):
    keys = (pk_match.group(0).replace("((", "(").split("(")[1].replace("}) {", "}, {").replace(")", "")).split(",")
    for key in keys:
      key = key.replace(" ", "")
      filled_parameter = re.match(".+\{(.*)\}.*", key)
      if(filled_parameter):
        if(filled_parameter.group(1)=="u"):
          self.node.constraints.append(key.split("{")[0])
    
  def build_relationships(self, cypher_file, info_tuple, nodes_structure):
    nodes = nodes_structure
    head_node = None
    for node in nodes:
      if (node.constraints):
        head_node = node
    Builder = namedtuple('Builder', 'node_name csv_file column_size node_structure')
    for element in info_tuple:
      element = Builder(element[0], element[1], element[2], element[3])
      if(element.node_structure.label == head_node.label):
        continue
      path = "file://" + os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
      rel_type = self.generate_rel_type(self.remove_composed_label(head_node.label), self.remove_composed_label(element.node_structure.label))
      idx = element.node_structure.properties.keys().index(head_node.constraints[0])
      query = "LOAD CSV FROM \'{path}/{csv_file}\' AS line MERGE (h{hlabel} {{ {key}:line[{idx}] }})-[:{rel_type}]->(n{nlabel} {{ {key}:line[{idx}] }});\n".format(path=path, csv_file=element.csv_file, hlabel=head_node.label, key=head_node.constraints[0], idx=idx, rel_type=rel_type, nlabel=element.node_structure.label)
      cypher_file.write(query)

  def remove_composed_label(self, label):
    label = label.split(":")[-1]
    return label

  def generate_rel_type(self, clabel, nlabel):
    rel_type = clabel + "_" + nlabel
    return rel_type

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

