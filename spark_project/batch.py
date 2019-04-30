from collections import defaultdict
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from kafka import KafkaConsumer
from kafka import KafkaProducer
import kafka.errors

from hdfs import InsecureClient

import os
import time
import json
import sys
import subprocess
import signal

TOPIC_RESULT = os.environ['K_TOPIC_RESULT']
TOPIC_START = os.environ['K_TOPIC_START']
KAFKA_HOST = os.environ['KAFKA_HOST']

################################################ Functions and Classes ##############################################
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def connectKafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer( TOPIC_START, 
                                      bootstrap_servers=os.environ['KAFKA_HOST'])
                                    #   auto_offset_reset='earliest',
                                    #   enable_auto_commit=True)
            print("CONSUMER: Connected to Kafka!")

            return consumer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            sleep(3)

def conectKafkaProducer():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
            print("PRODUCER: Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)

def connect_to_hdfs():
    while True:
        try:
            hdfs = InsecureClient(os.environ['HDFS_HOST'], user='root')
            return hdfs
        except:
            print "Insecure Client HDFS ERROR...try again"
            time.sleep(3)
            
class MyNode:

    def __init__(self, lat=float(0), lon=float(0) ):
        self.lat = lat
        self.lon = lon
        self.neighbors_in = list()
        self.neighbors_out = list()



def init(key, value, start_node_ids, end_node_ids):
    # Here you can specify acctually more start & end nodes        
    if key in start_node_ids:        
        value["path_start"] = [ (0 , "" ) ]
    else:
        value["path_start"] = [ (sys.maxint, "") ]

    if key in end_node_ids:
        value["path_end"] = [ (0 , "" ) ]
    else:
        value["path_end"] = [ (sys.maxint, "") ]

    return key, value

def checkIDs(keys, id_list):
    for n_id in id_list:
        if n_id not in keys:
            return False
    return True

def updatePaths(current_path, neighbor_weight, node_id, from_start):
        
    weight = int(current_path[0]) + int(neighbor_weight)

    if from_start == True :            
        # path = current_path[1] + " " + str(node_id) + " " + str(neighbor_weight) + " "
        path = current_path[1] + str(node_id) + " " + str(neighbor_weight) + " "

    else:
        # path = " " + str(neighbor_weight) + " " + str(node_id) + " " + current_path[1]
        path = " " + str(neighbor_weight) + " " + str(node_id) + current_path[1]
    
    return (weight, path)


def Map(node_id, node):
    '''node is dict -> lon, lat, neighbors_in, neighbors_out'''
    # = node_in[0]
    #node = node_in[1]
 
    neighbors_list = []

    # front propagation
    dist = node["path_start"][0][0]
    if dist != sys.maxint:

        for neighbor in node["neighbors_out"]:
            # Send every path from previous node to neighbors
            path = [ updatePaths(current_path=p, neighbor_weight=neighbor["weight"], node_id=node_id, from_start=True) \
                        for p in node["path_start"] if p[0]!=sys.maxint 
                        ]
             
            neighbors_list.append( ( unicode( neighbor["id"] ), \
                                    {"path_start": path, "path_end": [ (sys.maxint, "") ] }  ))
    
    dist = node["path_end"][0][0]
    if dist != sys.maxint:
        
        for neighbor in node["neighbors_in"]:
            
            # Send every path from previous node to neighbors
            path = [ updatePaths(current_path=p, neighbor_weight=neighbor["weight"], node_id=node_id, from_start=False) \
                        for p in node["path_end"] if p[0]!=sys.maxint 
                        ]

            neighbors_list.append( ( unicode( neighbor["id"] ), \
                                    {"path_end": path, "path_start": [ (sys.maxint, "") ] }  ))

    # print "\n", node_id, " Neighbors list ", neighbors_list,"\n"

    return neighbors_list

    
def Reduce(a, b):
    # both a and b has filled every atribute
    ''' dict= lon, lat, neighbors_in, neighbors_out, path_start, path_end'''
    if a == None:
        return b
    if b == None:
        return a

    a_path_start = a["path_start"]
    a_path_end = a["path_end"]

    a.update(b)
    a["path_start"] += b["path_start"]
    a["path_end"] += b["path_end"]

    return a


def meregeWithBase(a, b):
    # eliminate None, and solve situation if you have (286, basic, start), (285, basic, end)
    if a == None:
        return b
    if b == None:
        return a

    # a is basic record, put path on it
    if "lon" in a:        
        a["path_start"] += b["path_start"]
        a["path_end"] += b["path_end"]
        return a
    else:
        b["path_start"] += a["path_start"]
        b["path_end"] += a["path_end"]
        return b

def connected(start_path=None, end_path=None, node=None):

    if node != None:
        #print node 

        start_path = node[1]["path_start"][0]
        end_path = node[1]["path_end"][0]

    if( start_path[0] != sys.maxint and end_path[0] != sys.maxint):
        return True
    else:
        return False 

def getResults(path_start, current_id, path_end, display=False):
    
    total_weight = path_start[0] + path_end[0]

    if display:
        print  "RESULT PATH from: "+ str(current_id)+ " --> START " + \
                path_start[1] + str(current_id) + path_end[1] + \
                " END | Distance = " + str(total_weight)+" "
    


    return total_weight, str(total_weight)+" "+path_start[1] + str(current_id) + path_end[1] 

def sortPaths(node_id, node):
    
    node["path_start"] = sorted( set(node["path_start"]), key = lambda x: x[0])[:3]
    node["path_end"]   = sorted( set(node["path_end"]), key = lambda x: x[0])[:3]
    
    return (node_id, node) 

def saveResults(node_id, node):
    # Map Node to result string Step
    # Create list of strings for every key
    results = []

    for start_path in node["path_start"] :
        for end_path in node["path_end"] :
        
            if connected(start_path=start_path, end_path=end_path) :
                results.append( getResults(path_start=start_path, current_id=node_id, \
                                             path_end=end_path, display=True )                                  
                )
                    
    return results


def getNodesKafka(consumer):
    
    nodes = consumer.poll(timeout_ms=500).values()
    if len(nodes) == 0:
        return None

    nodes_ids = nodes[-1][-1][6].split('\n')
    start_ids = nodes_ids[0].split(' ')
    end_ids = nodes_ids[1].split(' ')
    
    print "NODE IDS:\n", start_ids, end_ids

    return start_ids, end_ids 

def getNodesKaafkaBlocking(consumer):
    print "BATCH: Waiting for new Start-End Nodes..."

    while True:        
        start_end_nodes = getNodesKafka(consumer)
        if start_end_nodes != None:            
            return start_end_nodes


def findChildPID(parent_pid):
    print "BATCH: find Real-Time PID..."
    while True:
        try:
            child_id = subprocess.check_output( ["ps", "--ppid", str(parent_pid) ] )
                    # PID TTY TIME CMD
            child_id = child_id.split('\n')[1].split()[0]
            print "BATCH: Real-Time PID = ",child_id
            return int(child_id)
        except:
            print "NO CHILD PROCESS"

def readHDFS(spark, path):
    print "BATCH: READ FROM HDFS"
    while True:
        try:
            data = spark.read.options(multiline=True).json(path)                
            return data
        except:
            time.sleep(1)
            print "BATCH: NO DATA IN HDFS. start again"           
##########################################  CODE  ################################################################


    
def main(rt_process_id, start_node_ids=None, end_node_ids=None, steps=5):
    # Calculate all paths between start and end nodes,# 
    # Max node distance between start-end is 2*steps,
    # If Offset is set, it will cat nodes other from 
    # rectangle defined by start, end node position 

    print "BATCH STARTS >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> \n\n"

    rt_process_id = findChildPID(rt_process_id)

    hdfs = connect_to_hdfs()
    consumer = connectKafkaConsumer()
    producer = conectKafkaProducer()
    
    conf = SparkConf().setAppName("Shortest_Path").setMaster("local")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    quiet_logs(spark)
    
    # data = readHDFS(spark, path="hdfs://namenode:8020/data_in")
    # print sorted(data.select("key").collect()), "\n"
    # exit(1)

    data = defaultdict(MyNode)

    # Block until start/end node are available    
    start_end_nodes = getNodesKaafkaBlocking(consumer)

    # Batch - Lambda architecture
    while True:
        print "BATCH: new iteration"

        if start_end_nodes != None:
            start_node_ids = start_end_nodes[0]
            end_node_ids = start_end_nodes[1]

        print "start_node_ids = ", start_node_ids
        print "end_node_ids = ", end_node_ids, "\n"
        
        data = readHDFS(spark,path="hdfs://namenode:8020/data_in")
        print "HDFS DATA\n"
        # print type(data), data

        data_keys = sorted(data.select("key").collect())
        print "HDFS KEYS\n",type(data_keys), data_keys, "\n"
        

        if checkIDs(data_keys, start_node_ids) or checkIDs(data_keys, start_node_ids):
            print "No Such Nodes in Graph"
            start_end_nodes = getNodesKaafkaBlocking(consumer)
            continue
            

        rdd = data.rdd.map(lambda x: (x[0], x[1].asDict() ) )
        rdd = rdd.map(lambda x: init(x[0], x[1], start_node_ids, end_node_ids) )
        # print "DATA_IN: \n",rdd.collect()

        

        for i in range(steps):
            # Chackpoint - Kafka
            start_end_nodes = getNodesKafka(consumer)
            if start_end_nodes != None: break

            # FIRST MAP
            neighbors_list = rdd.flatMap(lambda x: Map( x[0], x[1] ) )

            # Imamo samo one koji su poslednji iterirani #pazi Zavisi da li imas Left ili Right JOIN
            rdd =  rdd.leftOuterJoin(neighbors_list)
            
                                            #  x [0]-key, x[1]-tuple(orig baza, new_node) 
            rdd = rdd.map(lambda x: (x[0], meregeWithBase( x[1][0], x[1][1] )  ) )

            rdd = rdd.reduceByKey(lambda x, y: Reduce(x, y) )        

            rdd = rdd.map(lambda x: sortPaths( x[0], x[1] ) )


        if start_end_nodes != None:             
            continue

        results = rdd.filter(lambda node: connected( node=node ) ) \
                    .map(lambda x: saveResults( x[0], x[1] ) ) \
                    .flatMap(lambda x: x) \
                    .distinct() \
                    .sortByKey(ascending=True) \
                    .map(lambda x: x[1] )
        
        
        res = results.collect()                

        if len(res) != 0:
            msg = res[0]        
            print "\n SHORTEST: ", msg, "\n"            
            producer.send( TOPIC_RESULT, value=msg, key=msg )

            print "ALL_RESULTS", type(res)
            for r in res:
                print r, "\n"

            with open('results.txt', 'w') as outfile: 
                outfile.write("\n".join( res ))
            hdfs.upload("/results", "./results.txt", overwrite=True )
            # Send signal to Real-time part to take new results
            os.kill(rt_process_id, signal.SIGUSR1)
            print ("BATCH ENDED SUCCESSFULLY \n")
        else:
            print "\n >>>>>>>>>>>>>>>>>>>>>> Shortest Path NOT FOUND !!! <<<<<<<<<<<<<<<<<<<<<<<\n\n"
            
            

        # Chackpoint - Kafka
        start_end_nodes = getNodesKafka(consumer)
        
    
        


if __name__ == "__main__":

    print "\n SYSTEM ARG: ", sys.argv

    if len(sys.argv) == 2:
        main(rt_process_id=int(sys.argv[1]))

    elif len(sys.argv) == 4:
        main(rt_process_id=int(sys.argv[1]),
             start_node_ids=int(sys.argv[2]),
             end_node_ids=int(sys.argv[3]) )
        
    elif len(sys.argv) == 5:
        main(rt_process_id=int(sys.argv[1]),
             start_node_ids=int(sys.argv[2]),
             end_node_ids=int(sys.argv[3]), 
             steps=sys.argv[4])
    else:
        sys.exit("FORMAT: [START_id END_id] [ offset > 1 ]")    
        

    
    
