from py2neo import Graph, Node, Relationship

import os


class Neo4j:

    graph = None # 图数据库neo4j

    def __init__(self, cfg):
        # 读取配置文件，链接数据库
        self.graph = Graph(host=cfg['host'], port=cfg['port'], user=cfg['username'], password=cfg['password'])

    def nodes_to_graph(self, nodes, data_type):
        pass

    def edges_to_graph(self, nodes, data_type):
        pass

    def matchItembyTitle(self, value):
        answer = self.graph.find_one(label="Item", property_key="title", property_value=value)
        return answer

    # 根据title值返回互动百科item
    def matchHudongItembyTitle(self, value):
        answer = self.graph.find_one(label="HudongItem", property_key="title", property_value=value)
        return answer
