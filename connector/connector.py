
#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.metadata import KeyspaceMetadata
import re
import sys
from io import open
from schema_parser import SchemaParser
from cypher_queries_generator import CypherQueriesGenerator
import csv
import codecs


cluster = Cluster()
session = cluster.connect('playlist')
meta_str = session.cluster.metadata.export_schema_as_string()
keyspace = session.cluster.metadata.keyspaces["playlist"]
if(len(sys.argv) <= 1):
  parser = SchemaParser(keyspace.export_as_string())
  parser.parse()
  sys.exit("Generated schema.yaml file.")

music_results_file = codecs.open('music_results.csv', encoding='utf-8', mode='w+')
rows = session.execute('SELECT * FROM track_by_id')
writer = csv.writer(music_results_file)
writer.writerow(['track_id', 'artist', 'genre', 'music_file', 'track', 'track_length_in_seconds'])
writer.writerows([(track.track_id, track.artist, track.genre, track.music_file, track.track, track.track_length_in_seconds) for track in rows])

artists_names_results_file = codecs.open('artists_names_results.csv', encoding='utf-8', mode='w+')
rows = session.execute('SELECT * FROM artists_by_first_letter')
writer = csv.writer(artists_names_results_file)
writer.writerow(['first_letter', 'artist'])
writer.writerows([(artistt.first_letter, artistt.artist) for artistt in rows])

cypher_queries_gen = CypherQueriesGenerator(keyspace)
cypher_queries_gen.generate()
cypher_queries_gen.build_queries(["track_by_id", "artists_by_first_letter"], ["music_results.csv", "artists_names_results.csv"])


class Neo4jNodeBuilder(object):
  def __init__(self, keyspace, schema):
    self.keyspace = keyspace
    self.schema = schema
