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
    print(nodes)
    self.assertEqual(len(nodes), 6)

  def test_build_queries(self):
    #filled YAML file analyser
    self.cypher_queries_gen = CypherQueriesGenerator(self.keyspace)
    nodes = self.cypher_queries_gen.generate()
    self.cypher_queries_gen.build_queries(nodes, ["track_by_id"], ["music_results.csv"])
    cypher_file = open('cypher_', 'r')
    statement = cypher_file.read()
    path = str(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
    expected = "MERGE (:playlist:track_by_id {{genre: line[2], artist: line[1], track: line[0], track_length_in_seconds: line[3], music_file: line[4], track_id: line[5]}} );\n".format(path=path)
    self.assertTrue(statement.endswith(expected))
    self.assertTrue(statement.startswith("LOAD CSV"))

if __name__ == '__main__':
  unittest.main()