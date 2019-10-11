from pyspark import SparkConf, SparkContext
import pyspark
import sys


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


def map_friends(line):
    """
    map function to construct edge between friends
    construct a pair of ((friend1, friend2) -> common friends list)
    if two friends are already direct friends, then common friends list
    is empty.
    :param line: tuple of (<User>, [friend1, friend2, ... ]),
                each user and a list of user's friends
    :return:
    """
    user = line[0]
    friends = line[1]
    yield ((user, user), [])
    for i in range(len(friends)):
        yield ((user, friends[i]), [])
        for j in range(len(friends)):
            yield ((friends[i], friends[j]), [user])
            yield ((friends[j], friends[i]), [user])


def reduce_friends_pair(pair1, pair2):
    """
    reduce function to reduce same friend-friend pairs.
    If the common friend list is None, which means they are direct friends,
    still return empty list
    :param pair1: reduceByKey first pair
    :param pair2: reduceByKey second pair
    :return: a pair of ((friend1, friend2) -> common friends list)
            if two friends are already direct friends,
            then common friends list is empty.
    """
    if len(pair1) == 0 or len(pair2) == 0:
        return []
    common_friends = set(pair1).union(set(pair2))
    return list(common_friends)


def find_mutual(line):
    """
    map mutual edges into (user, (friend, common_friend_num))
    :param line: a pair of ((friend1, friend2) -> common friends list)
            if two friends are already direct friends,
            then common friends list is empty.
    :return: (user, (friend, common_friend_num))
    """
    return line[0][0], (line[0][1], len(line[1]))


def sort_top_friends(line):
    """
    sort friend has most common friends
    :param line:  (user, [(friend, common_friend_num), ...])
    :return: recommendations result for the user
    """
    user = line[0]
    friends = sorted(line[1], key=lambda x: (x[1], -x[0]), reverse=True)
    while len(friends) > 0:
        if friends[-1][1] == 0:
            friends.pop()
        else:
            break
    if len(friends) > 10:
        friends = friends[0:10]
    return user, friends


def main():
    # Configure Spark
    sc = pyspark.SparkContext.getOrCreate()
    # The directory for the file
    filename = "q1.txt"

    # Get data in proper format
    data = getData(sc, filename)

    # Get set of all mutual friends
    mapData = data.flatMap(map_friends).reduceByKey(reduce_friends_pair)
    # print(mapData.take(10))
    # For each person, get top 10 mutual friends
    getFriends = mapData.map(find_mutual).groupByKey().map(sort_top_friends)
    # print(getFriends.take(5))
    # Only save the ones we want
    wanted = [924, 8941, 8942, 9019, 49824, 13420, 44410, 8974, 5850, 9993]
    result = getFriends.filter(lambda x: x[0] in wanted).collect()
    for res in result:
        print("For user %d:" % res[0])
        for recommendation in res[1]:
            print(recommendation)
        print()
    sc.stop()


if __name__ == "__main__":
    main()
