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
        continue
      end_pk_match = re.match(".*\sPRIMARY KEY:.*", line)
      if(end_pk_match):
        self.build_end_indexes(end_pk_match.group(0))
        continue
      self.collect_filled_parameters(line)
    return self.nodes

  def build_queries(self, tables, result_csvs):
    nodes = self.nodes
    cypher_file = open('cypher_', 'w+')
    columns = self.analyse_csv(result_csvs)
    nodes_structure = self.analyse_node(tables, nodes)
    # builds a tuple with all needed information to build a Cypher query
    Builder = namedtuple('Builder', 'node_name csv_file column_size node_structure')
    info_tuple = zip(tables, result_csvs, columns, nodes_structure)
    for element in info_tuple:
      element = Builder(element[0], element[1], element[2], element[3])
      if(element.node_structure.constraints):
        constraint_query = "CREATE CONSTRAINT ON (n:{label}) ASSERT n.{constraint} IS UNIQUE;\n".format(label=self.remove_composed_label(element.node_structure.label), constraint=element.node_structure.constraints[0])
        cypher_file.write(constraint_query)
      params_hash = self.build_params_hash(element.node_structure)
      params_hash_str = str(params_hash).replace("\'", "")
      unique_hash = self.build_unique_hash(element.node_structure)
      unique_hash_str = str(unique_hash).replace("\'", "")
      path = "file://" + os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
      query = "LOAD CSV WITH HEADERS FROM \'{path}/{csv_file}\' AS line MERGE (c{label} {uniques}) SET c += {params};\n".format(path=path, csv_file=element.csv_file, label=element.node_structure.label, uniques=unique_hash_str, params=params_hash_str)
      cypher_file.write(query)
    rel_tuple = zip(tables, result_csvs, columns, nodes_structure)
    self.build_relationships(cypher_file, rel_tuple, nodes_structure)

  def build_end_indexes(self, end_pk_match):
    keys = end_pk_match.lstrip().split(" PRIMARY KEY: ")
    key = keys[1].replace("{", "").replace("}", "")
    if(key=="u"):
      info_hash = (keys[0]).split(" ")
      self.node.constraints.append(info_hash[0])

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
    if (len(nodes_structure)<=1):
      return
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
      query = "LOAD CSV WITH HEADERS FROM \'{path}/{csv_file}\' AS line MERGE (h{hlabel} {{ {hkey}: line.{hkey} }}) MERGE (n{nlabel} {{ {nkey}: line.{nkey} }}) CREATE UNIQUE (n)-[:{rel_type}]->(h);\n".format(path=path, csv_file=element.csv_file, hlabel=head_node.label, hkey=head_node.constraints[0], nkey=element.node_structure.constraints[0], rel_type=rel_type, nlabel=element.node_structure.label)
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

  def build_unique_hash(self, node):
    params = {}
    for constraint in node.constraints:
      params.update({constraint: "line.{constraint}".format(constraint=constraint)})
    return params


  def build_params_hash(self, node):
    params = {}
    for key in node.properties.keys():
      params.update({key: "line.{key}".format(key=key)})
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

