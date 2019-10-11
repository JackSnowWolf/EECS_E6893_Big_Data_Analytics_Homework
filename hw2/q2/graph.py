import pyspark
from pyspark import SparkConf, SparkContext
from pyspark import SQLContext
import os
from graphframes import *


def getData(sc, filename):
    """
    Load data from raw text file into RDD and transform.
    Hint: transfromation you will use: map(<lambda function>).
    Args:
        sc (SparkContext): spark context.
        filename (string): hw2.txt cloud storage URI.
    Returns:
        RDD: RDD list of tuple of (<User>, [friend1, friend2, ... ]),
        each user and a list of user's friends
    """
    # read text file into RDD
    data = sc.textFile(filename)
    data = data.map(lambda line: line.split("\t")).map(
        lambda line: (int(line[0]), [int(x) for x in line[1].split(",")] if len(
            line[1]) else []))
    return data


def get_vertices(data, sqlcontext):
    """
    get vertices
    :param data: RDD list of tuple of (<User>, [friend1, friend2, ... ]),
        each user and a list of user's friends
    :param sqlcontext: SQLContext
    :return: dataframe
    """
    vertices = data.map(lambda line: (line[0],))

    return sqlcontext.createDataFrame(vertices, schema=["id"])


def get_edges(data, sqlcontext):
    """
    get edges
    :param data: RDD list of tuple of (<User>, [friend1, friend2, ... ]),
        each user and a list of user's friends
    :param sqlcontext: SQLContext
    :return:
    """

    def map_friends(line):
        """
        map function to construct edge between friends
        construct a pair of ((friend1, friend2) -> common friends list)
        if two friends are already direct friends, then common friends list
        is empty.
        :param line: tuple of (<User>, [friend1, friend2, ... ]),
                    each user and a list of user's friends
        :return: friend pair
        """
        user = line[0]
        friends = line[1]
        for i in range(len(friends)):
            yield (user, friends[i])
    edges = data.flatMap(map_friends)
    return sqlcontext.createDataFrame(edges, schema=["src", "dst"])


def connected_components(graph):
    pass

def page_rank(graph):
    pass

def main():
    # Configure Spark
    if not os.path.isdir("checkpoints"):
        os.mkdir("checkpoints")
    conf = SparkConf().setMaster('local').setAppName('connected components')
    sc = SparkContext(conf=conf)
    sqlcontext = SQLContext(sc)
    SparkContext.setCheckpointDir(sc, "checkpoints")

    # The directory for the file
    filename = "../q1/q1.txt"

    # Get data in proper format
    data = getData(sc, filename)
    edges = get_edges(data, sqlcontext)
    vertices = get_vertices(data, sqlcontext)
    graph = GraphFrame(vertices, edges)
    connected_components(graph=graph)


if __name__ == '__main__':
    main()
