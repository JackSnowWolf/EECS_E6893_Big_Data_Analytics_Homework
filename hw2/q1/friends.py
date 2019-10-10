from pyspark import SparkConf, SparkContext
import pyspark
import sys
from collections import defaultdict


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

    # TODO: implement your logic here

    return data


def mapFriends(line):
    """
    List out every pair of mutual friends, also record direct friends.
    Hint:
    For each <User>, record direct friends into a list:
    [(<User>, (friend1, 0)),(<User>, (friend2, 0)), ...],
    where 0 means <User> and friend are already direct friend,
    so you don't need to recommand each other.

    For friends in the list, each of them has a friend <User> in common,
    so for each of them, record mutual friend in both direction:
    (friend1, (friend2, 1)), (friend2, (friend1, 1)),
    where 1 means friend1 and friend2 has a mutual friend <User> in this "line"

    There are possibly multiple output in each input line,
    we applied flatMap to flatten them when using this function.
    Args:
        line (tuple): tuple in data RDD
    Yields:
        RDD: rdd like a list of (A, (B, 0)) or (A, (C, 1))
    """
    friends = line[1]
    for i in range(len(friends)):
        # Direct friend
        # TODO: implement your logic here

        for j in range(i + 1, len(friends)):
            # Mutual friend in both direction
            # TODO: implement your logic here
            pass


def findMutual(line):
    """
    Find top 10 mutual friend for each person.
    Hint: For each <User>, input is a list of tuples of friend relations,
    whether direct friend (count = 0) or has friend in common (count = 1)

    Use friendDict to store the number of mutual friend that the current <User>
    has in common with each other <User> in tuple.
    Input:(User1, [(User2, 1), (User3, 1), (User2, 1), (User3, 0), (User2, 1)])
    friendDict stores: {User2:3, User3:1}
    directFriend stores: User3

    If a user has many mutual frineds and is not a direct frined, we recommend
    them to be friends.

    Args:
        line (tuple): a tuple of (<User1>, [(<User2>, 0), (<User3>, 1)....])
    Returns:
        RDD of tuple (line[0], returnList),
        returnList is a list of recommended friends
    """
    # friendDict, Key: user, value: count of mutual friends
    friendDict = defaultdict(int)
    # set of direct friends
    directFriend = set()
    # initialize return list
    returnList = []

    # TODO: Iterate through input to aggregate counts
    # save to friendDict and directFriend

    # TODO: Formulate output

    return (line[0], returnList)


def main():
    # Configure Spark
    sc = pyspark.SparkContext.getOrCreate()
    # The directory for the file
    filename = "your q1.txt cloud storage URI"

    # Get data in proper format
    data = getData(sc, filename)

    # Get set of all mutual friends
    mapData = data.flatMap(mapFriends).groupByKey()

    # For each person, get top 10 mutual friends
    getFriends = mapData.map(findMutual)

    # Only save the ones we want
    wanted = [924, 8941, 8942, 9019, 49824, 13420, 44410, 8974, 5850, 9993]
    result = getFriends.filter(lambda x: x[0] in wanted).collect()

    sc.stop()


if __name__ == "__main__":
    main()
