# -*- coding: utf-8 -*-

import py2neo
import csv
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


def fetch_node2csv(src_file, data_type, file_dir):
    """从 原始文件 抽取 图 的 结点

    :return: 结点生成器
    """

    node_csv = open(os.path.join(file_dir, '_tp_nodes.csv'), 'w')
    _w = csv.writer(node_csv)
    file_header = ["id", "url", "name"]
    _w.writerow(file_header)
    if data_type is 'wiki_category':
        _id_prefix = 'wc'
        v = set()
        with open(src_file) as _f:
            for line in _f:
                _tp = line.strip().split("\t")
                # id    url title   son_id  son_url son_title
                if _tp[0] not in v:
                    _w.writerow([_id_prefix+"_"+_tp[0], _tp[1], _tp[2]])
                    v.add(_tp[0])

                if _tp[3] not in v:
                    _w.writerow([_id_prefix+"_"+_tp[3], _tp[4], _tp[5]])
                    v.add(_tp[3])

    node_csv.close()
    print('finish fetching nodes from:', src_file)
    print('position of nodes file :', node_csv)

    return


def fetch_edge2csv(src_file, data_type, file_dir):
    """从 原始文件 抽取 图 的 边

    :return: 边的生成器
    """

    edge_csv = open(os.path.join(file_dir, '_tp_edges.csv'), 'w')
    _w = csv.writer(edge_csv)
    file_header = ["item1", "rel", "item2"]
    _w.writerow(file_header)

    if data_type is 'wiki_category':
        _id_prefix = 'wc'
        v = set()
        with open(src_file) as _f:
            for line in _f:
                _tp = line.strip().split("\t")
                # id    url title   son_id  son_url son_title
                if (_tp[0], _tp[3]) not in v:
                    v.add((_tp[0], _tp[3]))
                    _w.writerow([_id_prefix+"_"+_tp[0], "下级分类", _id_prefix+"_"+_tp[3]])
                    _w.writerow([_id_prefix+"_"+_tp[3], "上级分类", _id_prefix+"_"+_tp[0]])

    edge_csv.close()

    print('finish fetching edges from:', src_file)
    print('position of edges file :', edge_csv)

    return


def baike_category2neo4j(cfg, data_type):
    """ 把百科的分类结构存入分成结点和边，分别存入import 文件，然后用import文件导入数据

    The full set of `data_type` supported are:

    =====================  =============================================
    Keyword                  Description
    =====================  =============================================
    ``wiki_category``        wikipedia category pages
    ``hudong_category``      hudong baike category pages
    ``baidu_category``       baidu baike category pages
    =====================  =============================================

    :return:
    """
    cfg_src = cfg['SRC_FILE']
    # 临时文件存储位置
    file_dir = cfg['NEO4J']['import_path']

    # 读取数据源文件
    src_file = os.path.join(os.getcwd(), cfg_src[data_type])

    # 从 源文件 抽取 结点
    fetch_node2csv(src_file, data_type, file_dir)
    # 从 源文件 抽取 边
    fetch_edge2csv(src_file, data_type, file_dir)

    return


def load2neo4j_nodes(config, data_type):
    """ 载入 图 的 节点

    :param config:
    :param data_type:
    :param file_path:
    :return:
    """
    print("begin loading nodes.")
    neo4j_handler = Neo4j(config['DEFAULT'], data_type)
    neo4j_handler.graph.run("LOAD CSV WITH HEADERS FROM \"file:///_tp_nodes.csv\" AS line "
                            "CREATE ( :"+data_type+" {name:line.name,id:line.id,url:line.url})")
    neo4j_handler.graph.run("CREATE CONSTRAINT ON (c:"+data_type+") ASSERT c.id IS UNIQUE")
    print("finish loading nodes.")

    return


def load2neo4j_edges(config, data_type):
    """ 载入 图 的 边

    :param config:
    :param data_type:
    :param file_path:
    :return:
    """

    print("begin loading edges.")
    neo4j_handler = Neo4j(config['DEFAULT'], data_type)
    neo4j_handler.graph.run("LOAD CSV WITH HEADERS FROM \"file:///_tp_edges.csv\" AS line "
                             "MATCH (entity1:" + data_type + "{id:line.item1}) , (entity2:" + data_type + "{id:line.item2}) "
                             "CREATE (entity1)-[:RELATION { type: line.rel}]->(entity2)")
    print("finish loading edges.")
    return


if __name__ == "__main__":
    dt = 'wiki_category'

    # 读取配置信息
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    baike_category2neo4j(cfg, dt)

    """为了效率，用命令行执行载入数据的操作
        分开执行，下面两部操作，效率最高，原因不详
    
    """
    load2neo4j_nodes(cfg, dt)
    #load2neo4j_edges(cfg, dt)
