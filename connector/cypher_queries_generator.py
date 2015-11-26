import re
import os
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

  # def build_queries(self, nodes, [track_by_id, artists_by_first_letter], ["music_results.csv", "artists_names_results.csv"])
  def build_queries(self, nodes, tables, result_csvs):
    cypher_file = open('cypher_', 'w+')
    columns = self.analyse_csv(result_csvs)
    info_tuple = zip(tables, result_csvs, columns)
    print(info_tuple)
    # for idx, unique_name in enumerate(tables):
    #   node = self.get_node(unique_name, nodes)
      # query = "LOAD CSV FROM " + result_csvs[idx] + "AS LINE CREATE (n" + node.label + "{ first_letter: line[0], artist: line[1] } );"

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
      if(filled_parameter.group(1) == "p"):
        self.node.properties.update({data.split(" ")[0]:data.split(" ")[1]})
      else:
        self.node.relationships.update({data.split(" ")[0]:data.split(" ")[1]})

