#!/usr/bin/env python

import re
import os

class SchemaParser(object):
  def __init__(self, schema):
    self.schema_str = schema

  def parse(self):
    schema_file = open('schema_', 'w+')
    schema_file.write(self.schema_str)
    schema_file = open('schema_', 'r')
    yaml_file = open(os.path.join(os.getcwd(),'schema.yaml'), 'w+')
    for line in schema_file:
      create_table_match = re.match("CREATE TABLE (.[^\(]*)", line)
      if (create_table_match):
        yaml_file.write((create_table_match.group(0)).rstrip())
        yaml_file.write(":\n")
        continue
      close_parentheses_match = re.match(".*AND.*", line) or re.match("\).*", line)
      if (close_parentheses_match):
        continue
      pk_match = re.match(".*PRIMARY KEY \(.*", line)
      if (pk_match):
        formatted_line = line.replace(","," {},")
        formatted_line = formatted_line.replace(")"," {})")
        yaml_file.write(formatted_line)
        continue
      newline_match = re.match("\n", line)
      if (newline_match):
        continue
      formatted_line = line.replace(",","")
      formatted_line = formatted_line.replace("\n", ": {}\n")
      yaml_file.write(formatted_line)
    self.add_neo4j_credentials(yaml_file)

  def add_neo4j_credentials(self, yaml_file):
    yaml_file.write("NEO4J CREDENTIALS (url {}, user {}, password {}) \n")
