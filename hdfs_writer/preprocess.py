import csv
import pandas
import json
from collections import defaultdict
import sys


num_lines = sys.maxint
cols = (['duration_sec','start_station_id', 'start_station_latitude', 'start_station_longitude', \
                        'end_station_id', 'end_station_latitude', 'end_station_longitude'])

                        
data_file = int(raw_input("Data 1-bike_proba.csv 2-bike.csv : ").split()[0] )
if data_file == 1:
    df = pandas.read_csv('../data/bike_proba.csv',sep=',', usecols=(cols))

elif data_file == 2:
    num_lines = int( raw_input("Number of rows to get from CSV: ").split()[0] )
    
    num_lines = sys.maxint if num_lines < 0 else num_lines 

    df = pandas.read_csv('../data/bike.csv',sep=',', usecols=(cols))
else:
    sys.exit("Uneti format nije dobar")


class MyNode:

    def __init__(self, lat, lon ):
        self.lat = lat
        self.lon = lon
        self.neighbors_in = list()
        self.neighbors_out = list()
'''
    def toJSON(self, file_out):
        return json.dumps(self, file_out, default=lambda o: o.__dict__,
                    sort_keys=True, ident=4)
'''
data = defaultdict( MyNode )

for i , row in df.iterrows():

    if int(row['start_station_id']) not in data:
        data[int(row['start_station_id'])] = MyNode( lat=float(row['start_station_latitude']),\
                                                     lon=float(row['start_station_longitude']))
        #data[int(row['start_station_id'])].lat = float(row['start_station_latitude'])
        #data[int(row['start_station_id'])].lon = float(row['start_station_longitude'])
    

    data[ int(row['start_station_id']) ].neighbors_out.append(
            (        
            {
            'id': str(int(row['end_station_id'])),
            'weight': int(row['duration_sec']),
            'lon': float(row['end_station_longitude']),
            'lat': float(row['end_station_latitude'])
            })           
    )
    print i
    if i == num_lines :
        break 


for i, row in df.iterrows():

    if int(row['end_station_id']) not in data:
        data[int(row['end_station_id'])] = MyNode( lat=float(row['end_station_latitude']),\
                                                     lon=float(row['end_station_longitude']))

    data[ int(row['end_station_id']) ].neighbors_in.append(
            (        
            {
            'id': str(int(row['start_station_id'])),
            'weight': int(row['duration_sec']),
            'lon': float(row['start_station_longitude']),
            'lat': float(row['start_station_latitude'])
            })        
    )
    if i == num_lines :
        break 

print sorted( [int(k) for k in data.keys()] )

# import jsbeautifier
# opts = jsbeautifier.default_options()
# opts.indent_size = 0
# out = jsbeautifier.beautify(json.dumps(data), opts)

with open('data.txt', 'w') as outfile: 
    # outfile.write("{") 
    for k, v in data.items():                
        json.dump({"key":k, "val":v}, outfile, default=lambda o: o.__dict__, sort_keys=True)#, indent=4)
        outfile.write(" \n")
    
    # outfile.write("}")
    # json.dump(data, outfile, default=lambda o: o.__dict__, sort_keys=True)#, indent=4)







