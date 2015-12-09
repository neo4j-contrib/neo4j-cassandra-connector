import inspect, os
import re
from py2neo import Graph, authenticate

class CypherRunner(object):
  
  def __init__(self, config_hash):
    self.queries_file = open('cypher_', 'r+')
    self.config_hash = config_hash
    url = self.config_hash['url'].replace("\"", "")
    if(self.config_hash['user'] and self.config_hash['password']):
      self.authenticate_url()
    self.graph = Graph(url)

  def run(self):
    tx = self.graph.cypher.begin()
    for line in self.queries_file:
      constraint_match = re.match("CREATE CONSTRAINT ON (.*)", line)
      if(constraint_match):
        self.graph.cypher.execute(line)
      else:
        tx.append(line)
    tx.commit()

  def authenticate_url(self):
    auth_url = self.config_hash['url'].split("http://")[1].split('/')[0]
    authenticate(auth_url, self.config_hash['user'], self.config_hash['password'])


