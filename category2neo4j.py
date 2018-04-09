# -*- coding: utf-8 -*-

import py2neo
import os
import configparser
from neo4j_model import Neo4j

# =============================================================================
#  Version: 0.1 (April 9, 2018)
#  Author: zhaojiapeng (imnu_zjp@qq.com)
#
# =============================================================================

""" 将原始数据文件转换成能存入neo4j的格式。

Wiki Data：238600+ 个类别
Hudong Data： 个类别
Baidu Data： 个类别

"""


def fetch_node(src_file, data_type):
    """从 原始文件 抽取 图 的 结点

    :return: 结点生成器
    """
    pass


def fetch_edge(src_file, data_type):
    """从 原始文件 抽取 图 的 边

    :return: 边的生成器
    """
    pass


def category2neo4j(data_type):
    """ 把百科的分类结构存入neo4j

    The full set of `data_type` supported are:

    =====================  =============================================
    Keyword                  Description
    =====================  =============================================
    ``wiki_category``        wiki pedia category pages
    ``hudong_category``      hudong baike category pages
    ``baidu_category``       baidu baike category pages
    =====================  =============================================

    :return:
    """

    # 读取配置信息
    config = configparser.ConfigParser()
    config.read('config.ini')
    cfg_src = config['SRC_FILE']

    # 读取数据源文件
    src_file = os.path.join(os.getcwd(), cfg_src[data_type])

    # 从 源文件 抽取 结点
    nodes = fetch_node(src_file, data_type)
    # 从 源文件 抽取 边
    edges = fetch_edge(src_file, data_type)

    # 导入图数据库
    neo4j_handler = Neo4j(config)
    neo4j_handler.nodes_to_graph(nodes, data_type)
    neo4j_handler.edges_to_graph(edges, data_type)
    print(src_file)


dt = 'wiki_category'
category2neo4j(dt)
