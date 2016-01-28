# Neo4j Cassandra Connector

Welcome to [Neo4j](http://neo4j.com/) [Cassandra](http://cassandra.apache.org/) Connector. The main goal of this project is to provide a prototype for the task of importing a dataset from Cassandra into Neo4j. 

Please be aware that this project is still under development and at this first stage it will work mainly for this restricted data collection. Please check our [docs](docs/cassandra_connector_doc.adoc) to have more information of the covered scenarios and a detailed instruction set for using this tool.


## Example usage

This example shows how to use the Cassandra Neo4j Connector to convert the music playlist dataset from Cassandra into Neo4j:

1. Ensure Cassandra and Neo4j are running
1. Clone this repository: `git clone https://github.com/neo4j-contrib/neo4j-cassandra-connector.git`
1. `cd neo4j-cassandra-connector/connector`
1. `pip install -r requirements.txt` - note only Python 3.4+ is currently supported
1. `python connector.py parse -k playlist` - this will parse the schema metadata for the specified Cassandra keyspace and generate a file `schema.yaml`. Edit this file to specify the graph structure you would like to use to import your data. See example here. An example is provided: `cp schema.yaml.example schema.yaml`
1. `python connector.py export -k playlist -t track_by_id,artists_by_first_letter` - this will export the data from Cassandra using CSV as an intermediary and import the data into Neo4j per the specified graph data model.

## TODO

- [ ] Allow user specified CQL queries. Currently the full table is exported, however a custom CQL query can be substituted [here](https://github.com/neo4j-contrib/neo4j-cassandra-connector/blob/master/connector/connector.py#L52).
- [ ] Python 2.x support. Currently only Python 3.x is supported.
- [ ] Add test suite.
- [ ] More robust graph model configuration. See [documentation](https://github.com/neo4j-contrib/neo4j-cassandra-connector/blob/master/docs/cassandra_connector_doc.adoc#123-filling-up-schemayaml-file) for column to property graph mapping confiruation options.

## License

   Licensed under the Apache License, Version 2.0 (the "License").
   You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.