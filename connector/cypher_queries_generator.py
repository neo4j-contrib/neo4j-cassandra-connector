import re
import os

class CypherQueriesGenerator(object):
  def __init__(self, keyspace):
    self.keyspace = keyspace

  def generate(self):
    yaml_file = open(os.path.join(os.getcwd(),'schema.yaml'), 'r')
    for line in yaml_file:
      create_table_match = re.match("CREATE TABLE (.*)", line)
      if (create_table_match):
