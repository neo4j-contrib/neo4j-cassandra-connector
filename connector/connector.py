
#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.metadata import KeyspaceMetadata
import re
import sys
from schema_parser import SchemaParser
from cypher_queries_generator import CypherQueriesGenerator


cluster = Cluster()
session = cluster.connect('playlist')
meta_str = session.cluster.metadata.export_schema_as_string()
keyspace = session.cluster.metadata.keyspaces["playlist"]
if(len(sys.argv) <= 1):
  parser = SchemaParser(keyspace.export_as_string())
  parser.parse()

music_results_file = open('music_results.csv', 'w+')
rows = session.execute('SELECT * FROM track_by_id')
for track in rows:
  track_id = str(track.track_id)
  artist = (track.artist).encode('utf-8')
  genre = (track.genre).encode('utf-8')
  music_file = (track.music_file).encode('utf-8')
  trackk = (track.track).encode('utf-8')
  track_length_in_seconds = str(track.track_length_in_seconds)
  result = "{track_id}, {artist}, {genre}, {music_file}, {track}, {track_length_in_seconds} \n".format(track_id=track_id, artist=artist, genre=genre, music_file=music_file, track=trackk, track_length_in_seconds=track_length_in_seconds)
  music_results_file.write(result)
  
  # artists_results_file = open('artists_results_', 'w+')
  # rows = session.execute('SELECT * FROM track_by_artist')
  # for track in rows:
  #   results = (track.artist, track.track, track.track_id, track.music_file, track.starred, track.track_length_in_seconds)
  #   artists_results_file.write(str(results))
  #   artists_results_file.write("\n")

artists_names_results_file = open('artists_names_results.csv', 'w+')
rows = session.execute('SELECT * FROM artists_by_first_letter')
for artist in rows:
  first_letter = (artist.first_letter).encode('utf-8')
  artistt = (artist.artist).encode('utf-8')
  result = "{first_letter}, {artist} \n".format(first_letter=first_letter, artist=artistt)
  artists_names_results_file.write(result)

cypher_queries_gen = CypherQueriesGenerator(keyspace)
nodes = cypher_queries_gen.generate()
cypher_queries_gen.build_queries(nodes, ["track_by_id", "artists_by_first_letter"], ["music_results.csv", "artists_names_results.csv"])


class Neo4jNodeBuilder(object):
  def __init__(self, keyspace, schema):
    self.keyspace = keyspace
    self.schema = schema
