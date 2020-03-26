from py2neo import Graph, Database, Node, Relationship
import os
graph=Graph("http://localhost:7474",auth=("neo4j","Sanjana@123"))

def scope(data_file):
    import json
    json = json.load(data_file)
    query = """
        WITH {json} AS data
        UNWIND KEYS($json) AS k
        CREATE(label:author{name:$json[k].author,hashtag:$json[k].hashtags,tweetdate:$json[k].date,date_time:$json[k].datetime,tid:$json[k].tid,tweettext:$json[k].tweet_text,authorid:$json[k].author_id,location:$json[k].location,language:$json[k].lang,like_count:$json[k].like_count})
    """
    graph.run(query,json=json)

def problem1():
    for record in graph.run("MATCH (n) WHERE n.name='selfresqingprncess' RETURN n ORDER BY n.date_time DESC"):
        print(record["n"]["tid"])
        print(record["n"]["tweettext"])
        print(record["n"]["authorid"])
        print(record["n"]["location"])
        print(record["n"]["language"])

def problem2():
    for record in graph.run("MATCH (n) WHERE n.tweettext CONTAINS 'Senate' RETURN n ORDER BY n.like_count DESC"):
        print(record["n"]["tweettext"])

def problem3():
    for record in graph.run("MATCH (n) WHERE n.hashtag='pqr' RETURN n ORDER BY n.date_time DESC"):
        print(record["n"]["tweettext"])

def problem4():
    for record in graph.run("MATCH (n) WHERE n.tweettext CONTAINS 'selfresqingprncess' RETURN n ORDER BY n.date_time DESC"):
        print(record["n"]["tweettext"])

def problem5():
    for record in graph.run("MATCH (n) WHERE n.tweetdate='2017-11-26' RETURN n ORDER BY n.like_count DESC"):
        print(record["n"]["tweettext"])

def problem6():
    for record in graph.run("MATCH (n) WHERE n.location='Hollywood, California USA' RETURN n"):
        print(record["n"]["tweettext"])

def problem7():
    for record in graph.run("MATCH (n{tweetdate:'2017-11-26'})  DELETE n"):
        print('pqr')

path = "/home/sanjana/Documents/dataset/"
for filename in os.listdir(path):
    with open(path + filename) as data_file:
        scope(data_file)

problem1()
problem2()
problem3()
problem4()
problem5()
problem6()
problem7()
