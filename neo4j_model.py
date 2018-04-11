from py2neo import Graph, Node, Relationship

import os
import time

class Neo4j:

    graph = None # 图数据库neo4j
    id_prefix = None
    data_type = None

    def __init__(self, cfg, data_type='wiki_category'):
        # 读取配置文件，链接数据库
        self.graph = Graph(host=cfg['host'], port=cfg['port'], user=cfg['username'], password=cfg['password'])
        #self.graph.run()
        # 存入图数据库的id前缀
        if data_type is 'wiki_category':
            self.id_prefix = 'wc'
            self.data_type = data_type

    def nodes_to_graph(self, nodes):
        nth = 0
        for _node in nodes:
            # id    url title
            _node = Node(self.data_type, name=_node[2], id=self.id_prefix+"_"+_node[0], url=_node[1])
            self.graph.create(_node)
            nth += 1
            if nth%10000 == 0:
                print(nth)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        return

    def edges_to_graph(self, edges):
        nth = 0
        for _edge in edges:
            # id    url title   son_id  son_url son_title
            _node_a = self.graph.find_one(label=self.data_type, property_key="id", property_value=self.id_prefix + "_" + _edge[0])
            _node_b = self.graph.find_one(label=self.data_type, property_key="id", property_value=self.id_prefix + "_" + _edge[3])
            _r = Relationship(_node_a, 'hyponym', _node_b) # 正向关系
            __r = Relationship(_node_b, 'hypernym', _node_a) # 反向关系
            self.graph.create(_r)
            self.graph.create(__r)
            nth += 1
            if nth % 1000 == 0:
                print(nth)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        return



    def matchItembyTitle(self, value):
        answer = self.graph.find_one(label="Item", property_key="title", property_value=value)
        return answer

    # 根据title值返回互动百科item
    def matchHudongItembyTitle(self, value):
        answer = self.graph.find_one(label="HudongItem", property_key="title", property_value=value)
        return answer


