
#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.metadata import KeyspaceMetadata
import re
from schema_parser import SchemaParser

cluster = Cluster()
session = cluster.connect('playlist')
meta_str = session.cluster.metadata.export_schema_as_string()
keyspace = session.cluster.metadata.keyspaces["playlist"]
parser = SchemaParser(keyspace.export_as_string())
parser.parse()

music_results_file = open('music_results_', 'w+')
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

artists_names_results_file = open('artists_names_results_', 'w+')
rows = session.execute('SELECT * FROM artists_by_first_letter')
for artist in rows:
  first_letter = (artist.first_letter).encode('utf-8')
  artistt = (artist.artist).encode('utf-8')
  result = "{first_letter}, {artist} \n".format(first_letter=first_letter, artist=artistt)
  artists_names_results_file.write(result)


cypher_file = open('cypher_query_example', 'w+')
cypher_file.write("LOAD CSV FROM 'https://gist.githubusercontent.com/hannelita/ccc954d1b653664905b4/raw/c4582096a3e44ab75bd91359508515c9e24f6d8a/artists_by_first_letter.csv' AS line CREATE (:playlist:artist { first_letter: line[0], artist: line[1] } );")
cypher_file.write("\n \n")
cypher_file.write("LOAD CSV FROM 'https://gist.githubusercontent.com/hannelita/9cf983c3dceb1be9948d/raw/fb5da0181cf862ac27371d0606cb69939864c570/track_by_id.csv' AS line MATCH (b:playlist:artist) WHERE b.artist=line[1] CREATE (:playlist:track { track_id: line[0], artist: line[1], genre: line[2], music_file: line[3], track: line[4], track_length_in_seconds: toInt(line[5]) } )-[r:ARTIST_TRACK]->(b)")

class Neo4jNodeBuilder(object):
  def __init__(self, keyspace, schema):
    self.keyspace = keyspace
    self.schema = schema
