
#!/usr/bin/env python

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.metadata import KeyspaceMetadata

cluster = Cluster()
session = cluster.connect('mykeyspace')
rows = session.execute('SELECT user_id,  fname, lname FROM users')
meta_str = session.cluster.metadata.export_schema_as_string()
keyspace = session.cluster.metadata.keyspaces["mykeyspace"]
schema_file = open('schema_and_results', 'w+')
schema_file.write(keyspace.export_as_string())
schema_file.write("\n !!!!Results to graph!!!! ")
for user_row in rows:
  results = (user_row.user_id, user_row.fname, user_row.lname)
  schema_file.write(str(results))
  schema_file.write("\n")



class NodeBuilder(object):
  



