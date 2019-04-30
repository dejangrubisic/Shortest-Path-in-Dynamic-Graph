from hdfs import InsecureClient
import time
import os
import json

from kafka import KafkaConsumer
import kafka.errors

TOPIC_EDGES = os.environ['K_TOPIC_EDGES']


def connect_to_hdfs():
    while True:
        try:
            hdfs = InsecureClient(os.environ['HDFS_HOST'], user='root')
            return hdfs
        except:
            print "Insecure Client HDFS ERROR...try again"
            time.sleep(3)


def connect_to_kafka():
    while True:
        try:
            consumer = KafkaConsumer(   #TOPIC_EDGES, 
                                        bootstrap_servers=os.environ['KAFKA_HOST'],
                                        auto_offset_reset='earliest',
                                        enable_auto_commit=True)
            consumer.subscribe( [TOPIC_EDGES] )
            print("Connected to Kafka!")
            return consumer

        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)


def send_to_hdfs(data, hdfs):

    print "NEW DATA: "

    with open('data.txt', 'w') as outfile: 
        for k, v in data.items():                
            json.dump({"key":k, "val":v}, outfile, default=lambda o: o.__dict__, sort_keys=True)
            outfile.write("\n")

            print "key: ",k, " ==> ",  "val:",v
    print "\n"

    while True:
        try:
            hdfs.upload("/data_in", "./data.txt", overwrite=True )
            return
        except:
            print "Unable to write to HDFS "
            time.sleep(4)


def add_to_list(l, n_id, n_lon, n_lat, weight):

    exist_flag = False 
    # check if end_id is in neighbors_out
    for x in l:
        if x["id"] == n_id:
            exist_flag = True
            x["weight"] = int(weight)
    
    # add end_id to neighbors_out
    if exist_flag == False:
        l.append(
                { "id": n_id, "lon": n_lon, "lat": n_lat, "weight": int(weight) }
            )
    
    return l

def store_edge(edge, data):
    # edge = start_id weight end_id start_lon start_lat end_lon end_lat
    #           0       1       2       3        4         5       6
    
    start_coor = [ float(edge[3]), float(edge[4]) ]
    end_coor = [ float(edge[5]), float(edge[6]) ]

    if edge[0] in data.keys():
        data[edge[0]]["neighbors_out"] = add_to_list( l=data[edge[0]]["neighbors_out"],
                                                      n_id=edge[2],
                                                      weight=edge[1],
                                                      n_lon=end_coor[0],
                                                      n_lat=end_coor[1]                                                      
                                                    )       
    else:
        # add start_id to data if doesn't exist
        data[ edge[0] ] =   {   "lon": start_coor[0], "lat": start_coor[1], 
                                "neighbors_in":[], 
                                "neighbors_out":[{"id":edge[2], "lon": end_coor[0], "lat": end_coor[1], "weight": int(edge[1]) }]
                            }

    # check if end_id has start_id in neighbors_in
    #parallelize this
    if edge[2] in data.keys():
        data[edge[2]]["neighbors_in"] = add_to_list(  l=data[edge[2]]["neighbors_in"],
                                                      n_id=edge[0],
                                                      weight=edge[1],
                                                      n_lon=start_coor[0],
                                                      n_lat=start_coor[1]                                                      
                                                    )
    else:
        # add end_id to data if doesn't exist
        data[ edge[2] ] =   {   "lon": end_coor[0], "lat": end_coor[1], 
                                "neighbors_in":[{"id":edge[0], "lon": start_coor[0], "lat": start_coor[1], "weight": int(edge[1]) }], 
                                "neighbors_out":[]
                            }

    return data

def consume(data, consumer, hdfs, max_tuples = 5, max_time=2):
    print "Consume:"
    changes = 0
    current_seconds = 0
    write_seconds = 0

    try:
        while True:            
            edges = consumer.poll().values()            

            if len(edges) != 0:
                edges_list = []  

                # Have to make list to get all historical data
                # consumer gives only recent data otherwise
                for edge_info in edges[0]:
                    edges_list.append(edge_info[6].split())
                
                # Put new data to dict
                for edge in edges_list:
                    print "Consume_EDGE: ", type(edge), edge
                    data = store_edge(edge, data)

                changes += len(edges_list)
            
            diff_time = current_seconds-write_seconds

            # print "changes: ", changes
            # print "diff_time: ", diff_time, '\n' 

            if changes > max_tuples or (changes != 0 and diff_time > max_time):
                send_to_hdfs(data, hdfs)
                write_seconds = current_seconds
                changes = 0

            time.sleep(1)
            current_seconds += 1

        
    except KeyboardInterrupt:
        print "LEFT CONSUME \n"        
        pass

def main():
    hdfs = connect_to_hdfs()
    consumer = connect_to_kafka()

    with open("data.json") as json_f:
        data = json.load(json_f)

    send_to_hdfs(data, hdfs)

    consume(data, consumer, hdfs)

if __name__ == '__main__':
    # time.sleep(60)
    main()
    
