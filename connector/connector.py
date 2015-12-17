#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.metadata import KeyspaceMetadata
from cassandra.metadata import TableMetadata
import re
import sys
from io import open
from schema_parser import SchemaParser
from cypher_queries_generator import CypherQueriesGenerator
import csv
import codecs
import argparse

class CassandraConnector(object):
  """
    Handles exporting data from Cassandra to Neo4j
    using CSV as intermediary

  """

  def __init__(self, keyspace_name, tables=None, queries=None):
    self.KEYSPACE = keyspace_name
    self.session = Cluster().connect(self.KEYSPACE)
    self.keyspace_metadata = self.session.cluster.metadata.keyspaces[self.KEYSPACE]
    self.tables = tables

  def getTables(self):
    if self.tables:
      return self.tables
    else:
      return self.session.cluster.metadata.keyspaces[self.KEYSPACE].tables.keys()

  def getColumnsForTable(self, table):
    return self.session.cluster.metadata.keyspaces[self.KEYSPACE].tables[table].columns.keys()

  def parse(self):
    keyspace = self.session.cluster.metadata.keyspaces[self.KEYSPACE]
    parser = SchemaParser(keyspace.export_as_string())
    parser.parse()
    sys.exit("Generated schema.yaml file.")

  def export(self):
    tableNames = self.getTables()
    fileNames = [t + "_results.csv" for t in tableNames]

    for t in tableNames:
      results_file = codecs.open(t + "_results.csv", encoding='utf-8', mode='w+')
      rows = self.session.execute('SELECT * FROM ' + t)
      writer = csv.writer(results_file)
      writer.writerow(self.getColumnsForTable(t))
      writer.writerows([(e for e in row) for row in rows])

    cypher_queries_gen = CypherQueriesGenerator(self.keyspace_metadata)
    cypher_queries_gen.generate()
    cypher_queries_gen.build_queries(tableNames, fileNames)

class Neo4jNodeBuilder(object):
  def __init__(self, keyspace, schema):
    self.keyspace = keyspace
    self.schema = schema

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("action", type=str, help="Specify action, 'parse' to generate schema.yaml file. Then 'export' to export to Neo4j.", choices=["parse", "export"])
  parser.add_argument("-k", "--keyspace", type=str, help="Specify the Cassandra keyspace. If no keyspace is specified then 'playlist' will be used by default")
  parser.add_argument("-t", "--tables", type=str, help="Specify Cassandra table(s) to export to Neo4j in a comma separated list. If not specified then all tables will be exported.")

  args=parser.parse_args()
  keyspace = ''
  if args.keyspace:
    keyspace = args.keyspace
  else:
    keyspace = 'playlist'

  if args.action == "parse":
    connector = CassandraConnector(keyspace)
    connector.parse()
  elif args.action == 'export':
    if args.tables:
      tables = args.tables.split(",")
      connector = CassandraConnector(keyspace, tables=tables)
    else:
      connector = CassandraConnector(keyspace)

    connector.export()

class Neo4jNodeBuilder(object):
  def __init__(self, keyspace, schema):
    self.keyspace = keyspace
    self.schema = schema
