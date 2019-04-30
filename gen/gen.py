
from hdfs import InsecureClient
from kafka import KafkaProducer
import kafka.errors

from json import loads
import time
import random
import os
import sys


TOPIC_EDGES = os.environ['K_TOPIC_EDGES']
TOPIC_RESULT = os.environ['K_TOPIC_RESULT']

def connect_to_hdfs():
    while True:
        try:
            hdfs = InsecureClient(os.environ['HDFS_HOST'], user='root')
            return hdfs
        except:
            print "Insecure Client HDFS ERROR...try again"
            time.sleep(3)

def conectKafkaProducer():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
            print("PRODUCER: Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)

def main(start_node=None, end_node=None, inc=None):

    user_mode = start_node != None and end_node != None and inc != None

    hdfs = connect_to_hdfs()
    producer = conectKafkaProducer()

    data = dict()
    with hdfs.read('/data_in', encoding='utf-8', delimiter='\n') as reader:
        for line in reader: 
            print line       
            try:
                d = loads(line)
                data.update({unicode(d["key"]): d["val"]})
            except:
                pass


    print '\n', data
    # edge =    start_id  weight    end_id  start_lon start_lat end_lon    end_lat
    #               0       1          2        3         4         5         6
    edges = [ [x_id, int(y["weight"]), y["id"], x["lon"], x["lat"], y["lon"], y["lat"] ]
                for x_id, x in zip(data.keys(), data.values())
                for y in x["neighbors_out"]
            ]

    new_edges = [
                    ['75', 850, '66', '-122.4002', '37.755', '-122.392740827108', '37.7787416115368'],
                    ['80', 100, '1', '-122.41', '37.775', '-122.405', '37.766'],
                    ['1', 200, '75', '-122.405', '37.766', '-122.4002', '37.755']
                ]




    if user_mode:
        try:
            user_edge = [x for x in edges if x[0]==start_node and x[2]==end_node][0]
        except:
            print "No such node in dictionary "
            print "DICT:", edges
            exit(1)


    i=0
    while True:
        if user_mode:
            user_edge[1] = abs(user_edge[1] + int(inc))
            edge = user_edge            
        else:              
            if i % 2 == 0:      
                edge = random.choice(edges)
            else:
                edge = random.choice(new_edges)
            # this will change also edges
            edge[1] = abs(edge[1] + random.randrange(-250, 250))

        msg = ' '.join(str(e) for e in edge)
        producer.send(TOPIC_EDGES, value=msg, key=msg)

        print i, " -> ", msg
        time.sleep(7)
        i +=1
    
if __name__ == "__main__":

    if len(sys.argv) == 1:
        mode = "random"
        main()

    elif len(sys.argv) == 4:
        start_node, end_node, inc = sys.argv[1:4]
        mode = "user"
        main(start_node, end_node, inc)

    else:
        print "Format [ start_node end_node increment ]"
        sys.exit(1)

   