#!/usr/bin/env python
# -*- coding: utf-8 -*- 
"""Unit tests - Neo4j DocManager."""
import sys
import logging
import os
import time
from connector.schema_parser import SchemaParser


sys.path[0:0] = [""]

class SchemaParserTestCase(unittest.TestCase):
  def setUp(self):
    schema_file = open('schema_', 'r')
    self.schema_str = schema_file.read()
    self.parser = SchemaParser(self.schema_str)

  def tearDown(self):
    print("Ending")

  def test_parse(self):
    self.parser.parse()
    yaml_file = open('schema.yaml', 'r')
    yaml = yaml_file.read()
    self.assertIsNot(yaml, None)


if __name__ == '__main__':
  unittest.main()