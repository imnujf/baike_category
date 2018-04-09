# -*- coding: utf-8 -*-

from py2neo import Graph, Node, Relationship
import os

a = Node('Person', name='Alice')
b = Node('Person', name='Bob')
r = Relationship(a, 'KNOWS', b)
print(a, b, r)

a['age'] = 20
b['age'] = 21
r['time'] = '2017/08/31'
print(a, b, r)

print(os.getcwd())