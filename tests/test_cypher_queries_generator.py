#!/usr/bin/env python
# -*- coding: utf-8 -*- 
"""Unit tests - Neo4j DocManager."""
import sys
import logging
import inspect, os
import time
import unittest
from connector.cypher_queries_generator import CypherQueriesGenerator


sys.path[0:0] = [""]

class CypherQueriesGeneratorTestCase(unittest.TestCase):
  def setUp(self):
    schema_filled_file = open(os.path.join("tests", 'schema.yaml'), 'r')
    self.keyspace = "playlist"

  def test_generate(self):
    #filled YAML file analyser
    self.cypher_queries_gen = CypherQueriesGenerator(self.keyspace)
    nodes = self.cypher_queries_gen.generate()
    self.assertEqual(len(nodes), 6)

  def test_analyse_csv(self):
    # check number of columns on each csv result file
    cypher_queries_gen = CypherQueriesGenerator(self.keyspace)
    nodes = cypher_queries_gen.generate()
    columns = cypher_queries_gen.analyse_csv(["music_results.csv"])
    self.assertEqual(columns, [6])

  def test_analyse_node(self):
    cypher_queries_gen = CypherQueriesGenerator(self.keyspace)
    nodes = cypher_queries_gen.generate()
    nodes = cypher_queries_gen.analyse_node(["track_by_id"], nodes)
    self.assertEqual(len(nodes), 1)
    print(nodes[0])

  def test_build_queries(self):
    #filled YAML file analyser
    self.cypher_queries_gen = CypherQueriesGenerator(self.keyspace)
    nodes = self.cypher_queries_gen.generate()
    self.cypher_queries_gen.build_queries(["track_by_id"], ["music_results.csv"])
    cypher_file = open('cypher_', 'r')
    statement = cypher_file.read()
    path = str(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
    self.assertTrue(statement.startswith("LOAD CSV WITH HEADERS"))

if __name__ == '__main__':
  unittest.main()