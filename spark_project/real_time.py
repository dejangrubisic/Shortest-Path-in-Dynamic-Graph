# from __future__ import print_function

import signal
import os
import sys
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import kafka.errors



TOPIC_RESULT = os.environ['K_TOPIC_RESULT']
TOPIC_EDGES = os.environ['K_TOPIC_EDGES']




def conectKafkaProducer():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
            print("PRODUCER: Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)




# Get or register a Broadcast variable
def getBestPaths(sparkContext):

    # if ('paths_info' not in globals()):
        
    paths_info = sparkContext.textFile("hdfs:/results")

    if len(paths_info.collect())==0:
        print("REAL-TIME: NO PATHS INFO IN HDFS \n")
        return None
        
    paths_info = paths_info.map(lambda x: str2num(x) ).sortByKey(ascending=True)
    
    print ("\n READ PATHS_INFO from HSFS \n\n",paths_info.collect() )

    globals()['paths_info'] =  paths_info 



    return globals()['paths_info']

def str2num( x ):
        
    path = [ int(num) for num in x.split() ]
    
    return ( path[0], path[1:])

def num2str(key, value):
    # print ("key= ",key, " val= ", value)
    s = ' '.join(map(str, value))
        
    return str(key) + " " + s

def changeEdges(path, from_id, to_id, new_edge_val):
    # Format: path = [total, (n0 w0 n1 w1 ...w_end n_end) ]
    total_distance = path[0]

    #get every second
    just_nodes = path[1][::2]

    for i, val in enumerate(just_nodes):
        # find from_id except in last iteration

        if val == from_id and i < len(just_nodes)-1 :            
            if just_nodes[ i+1 ] == to_id :
                
                total_distance += ( new_edge_val - path[1][2*i+1] )
                path[1][2*i+1] = new_edge_val
                    

    return total_distance, path[1]


def findEdge(path, b_new_edge):

                
    path = changeEdges(path=path, from_id=b_new_edge.value[0], 
                                        to_id=b_new_edge.value[2], 
                                        new_edge_val=b_new_edge.value[1]
                                        )

    
    return (path[0], path[1])



def main(host="localhost", port="kafka:9092", checkpoint="./checkpoint", output="./output"):

    print ("REAL-TIME STARTS >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n" )
    print ("Real-Time PID = ", os.getpid())

    # read_results = False
    globals()['read_results'] = False

    def receiveSignal(signum, frame):
        print("\n >>>>>>>>>>>>>>>>>>>>>>>>>>>> receiveSignal <<<<<<<<<<<<<<<<<<<<<<<<<<<<< \n\n")
        # global read_results
        # read_results = True
        globals()['read_results'] = True
        

    producer = conectKafkaProducer()
    signal.signal(signal.SIGUSR1, receiveSignal)
     
    
    sc = SparkContext(appName="Python_RealTimeShortestPath")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 2)

    edges = KafkaUtils.createDirectStream( ssc, [ TOPIC_EDGES ], {"metadata.broker.list": "kafka:9092"})
    edges.pprint()
    
    words = edges.flatMap(lambda line: line[0].split(" "))


    print (" Wait for signal... \n")   
    signal.pause()
    time.sleep(1)
    print("RT_WAKE UP ",  globals()['read_results'])

    def process(time, rdd):
        global msg
        # global read_results

        print ("********************** REAL-TIME Process *************************\n\n")  
        
        # Read SHORTEST PATH from HDFS on signal

        print("READ_RESULTS: ",  globals()['read_results'])
        if  globals()['read_results'] == True:
            print("NEW PATHS ++++ \n")
            paths_info = getBestPaths(rdd.context)
            globals()['read_results'] = False
        else:
            paths_info = globals()['paths_info']

        new_edge = rdd.collect()
        print (new_edge, " ",  len(new_edge) )
        

        if len( new_edge) != 0 or paths_info == None :
        # if False:               
            new_edge = [ int(new_edge[0]), int(new_edge[1]), int(new_edge[2]) ]

            b_new_edge = rdd.context.broadcast(new_edge)

             
            paths_info = paths_info.map(lambda x: findEdge(x, b_new_edge) ).sortByKey(ascending=True) 

            best_paths = paths_info.map(lambda x: (x[0], num2str(x[0], x[1] )) ).take(3)#.collect()
        
            print ("Best Paths: \n")
            for x in best_paths:
                print (x, "\n")

            msg = best_paths[0][1]


            if msg != "":
                print ("\n\n\n SEND___RESULT: ", msg, "\n\n\n" )
                producer.send( TOPIC_RESULT, value=msg, key=msg )

            
            globals()['paths_info'] = paths_info

                
            
    words.foreachRDD(process)


    ssc.start()
    ssc.awaitTermination()



if __name__ == "__main__":
    if len( sys.argv ) == 5 :
        host, port, checkpoint, output = sys.argv[1:]
        main(host, port, checkpoint, output)    
    else:        
        main()
    