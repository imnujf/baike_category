# -*- coding: utf-8 -*-

from py2neo import Graph, Node, Relationship
import os
import configparser
from neo4j_model import Neo4j

a = Node('Wiki_Category', name='123', id="123")
b = Node('Wiki_Category', name='456', id="456")
r = Relationship(a, 'KNOWS', b)
#print(a, b, r)

#a.add_label('Baidu_Category')
#a.add_label('Wiki_Category')
#a['age'] = 20
#b['age'] = 21
#r['time'] = '2017/08/31'
#print(a, c, r)

config = configparser.ConfigParser()
config.read('config.ini')

neo4j_handler = Neo4j(config['DEFAULT'])

#neo4j_handler.graph.delete_all()
#neo4j_handler.graph.create(a)
#neo4j_handler.graph.create(b)

#x = neo4j_handler.graph.find_one(label="Wiki_Category", property_key="id", property_value="123")
#y = neo4j_handler.graph.find_one(label="Wiki_Category", property_key="id", property_value=123)
#print(x)
#print(y)
#r = Relationship(x, 'KNOWS', y)
#neo4j_handler.graph.schema.drop_uniqueness_constraint('Wiki_Category', 'id')
#neo4j_handler.graph.create(r)
#neo4j_handler.graph.merge(r)
#neo4j_handler.graph.schema.create_uniqueness_constraint('Wiki_Category', 'id')
#self.graph.schema.create_uniqueness_constraint(data_type, 'id')
#

"""
for ii in neo4j_handler.graph.run("MATCH (n:wiki_category) WHERE n.name = '中国政治'"
                                  "MATCH p=(n)-[r:RELATION]->() WHERE r.type = '上级分类'"
                                  "RETURN p"):
    print(Node(ii))



"""

x = neo4j_handler.graph.find_one(label="wiki_category", property_key="name", property_value="中国政治")
print(x)
y = neo4j_handler.graph.data("MATCH (n:wiki_category) WHERE n.name = '中国政治'"
                             "MATCH p=(n)-[r:RELATION]->(d) WHERE r.type = '上级分类'"
                             "RETURN d.id")
for ii in y:
    print(ii)








