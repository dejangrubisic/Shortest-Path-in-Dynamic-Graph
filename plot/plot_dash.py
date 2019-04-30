#!/usr/local/bin/python

import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objs as go




#for concatanation: list(itertools.chain.from_iterable(a))
import itertools

from hdfs import InsecureClient

from kafka import KafkaConsumer
from kafka import KafkaProducer
import kafka.errors
import os
import time
from json import loads
from numpy import float_ as toFloat


# TOPICS = [ os.environ['K_TOPIC_RESULT'], os.environ['K_TOPIC_EDGES'] ]
TOPIC_EDGES = os.environ['K_TOPIC_EDGES']
TOPIC_RESULT = os.environ['K_TOPIC_RESULT']
TOPIC_START = os.environ['K_TOPIC_START']
KAFKA_HOST = os.environ['KAFKA_HOST']


# colors
red = 'rgb(188, 0, 0)'
blue = 'rgb(0, 0, 255)'
middle_blue = 'rgb(108, 163, 252)'
light_blue = 'rgb(170, 205, 255)'
yellow = 'rgb(255, 236, 66)'
white = 'rgb(255, 255, 255)'
black = 'rgb(0, 0, 0)'
n_click_previous = 0



def connect_to_hdfs():
    while True:
        try:
            hdfs = InsecureClient(os.environ['HDFS_HOST'], user='root')
            print "Connected to HDFS", os.environ['HDFS_HOST']
            return hdfs
        except:
            print "Insecure Client HDFS ERROR...try again"
            time.sleep(3)


def readHDFS(hdfs, path='/data_in'):
    data = dict()
        
    while True:
        try:
            print "READ from HDFS..."
            with hdfs.read(path, encoding='utf-8', delimiter='\n') as reader:
                for line in reader:
                    if line == '':
                        break        
                    d = loads(line)
                    data.update({unicode(d["key"]): d["val"]})
            return data
        except:
            time.sleep(1)


def conectKafkaConsumer():
    while True:
        try:
            consumer = KafkaConsumer( #TOPICS, 
                                      bootstrap_servers=KAFKA_HOST, 
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True)
            print("CONSUMER: Connected to Kafka!")
            consumer.subscribe( [TOPIC_EDGES, TOPIC_RESULT] )

            return consumer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)



def conectKafkaProducer():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
            print("PRODUCER: Connected to Kafka!")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)


def plotNodes(node_lon, node_lat, node_ids):
# Create nodes on plot
    return  go.Scatter( x=node_lon,
                        y=node_lat,
                        mode='markers+text',
                        name="BlueNodes",
                        marker=dict(
                            color= blue,# 28, 114, 255
                            symbol='circle',
                            size=35
                        ),
                        text= ['<b>'+x+'</b>' for x in node_ids ],
                        textposition='middle center',
                        textfont=dict(
                            # family='sans serif',
                            size=18,
                            color=white
                        )                          
                )



def plotLine(line_id, a, b, color=middle_blue):
    a = toFloat(a)
    b = toFloat(b)
    return go.Scatter(  x=[ a[0], b[0] ],
                        y=[ a[1], b[1] ],
                        mode='lines',
                        name='l'+line_id,       
                        line=dict(
                                color=color,
                                width=4
                                ),
                        connectgaps=True,                                          
            )

def plotWeight(line_id, a, b, w, color=yellow): #188, 229, 255
    a = toFloat(a)
    b = toFloat(b)
    return go.Scatter(  x= [ (a[0] + b[0]) / 2 ],
                        y= [ (a[1] + b[1]) / 2 ],
                        mode='markers+text',
                        marker=dict(
                            color= color,
                            symbol='hexagram',
                            size=35
                        ),
                        name='w'+line_id,
                        text= ['<b>'+str(w)+'</b>'],
                        textposition='middle center',
                        textfont=dict(
                            # family='sans serif',
                            size=18,
                            color=black
                        ),
                        connectgaps=True,                         
            )
            

def transformCordinates(a, b, shift, size):
    a = toFloat(a)
    b = toFloat(b)

    m_x = (3 * a[0] + 5 * b[0]) / 8
    m_y = (3 * a[1] + 5 * b[1]) / 8

    n_x = (a[0] + 7*b[0]) / 8
    n_y = (a[1] + 7*b[1]) / 8
    
    return [m_x, m_y], [n_x, n_y]

def plotArrow(line_id, a, b, color=black):  
    
    a, b = transformCordinates(a, b, shift=0.001, size = 0.001)
    
    return dict(    xref='x', yref='y',
                    axref='x', ayref='y',
                    x= a[0], y= a[1], 
                    ax = b[0], ay = b[1],
                    showarrow = True,                           
                    arrowcolor = color,
                    arrowsize = 70,
                    arrowwidth = 3,
                    arrowside = "start",
                    arrowhead = 1
                )

def drawArrow(fig_arrows):
    return go.Layout(   annotations= fig_arrows,
                        title = "Connected Graph between Stations",                 
                        autosize=False,
                        showlegend=False,
                        width=1000,
                        height=800,
                        margin=go.layout.Margin(
                            l=50,
                            r=50,
                            b=100,
                            t=100,
                            pad=4
                        ),

                        paper_bgcolor= light_blue,
                        plot_bgcolor=white
                    )


def createBasePlot(data):
    node_lon = [ x["lon"] for x in data.values() ]
    node_lat = [ x["lat"] for x in data.values() ]

    # Creates dictionary of lines [x1, y1, x2, y2] with weight
    # key format is start_id-end_id
    lines = { str(x_id)+"-"+str(y["id"]): [ [x["lon"], x["lat"]], 
                                            [y["lon"], y["lat"]], 
                                            y["weight"]
                                        ]                                       
                for x_id, x in zip(data.keys(), data.values())
                for y in x["neighbors_out"]
            }


    print "NODES COORDINATES:\n", node_lon, node_lat


    # Lines - paths between nodes
    fig_lines = [ plotLine(line_id=key, a=line[0], b=line[1]) 
                for key, line in zip(lines.keys(),lines.values()) ]

    # Nodes - Vertices on graph
    fig_nodes = [ plotNodes(node_lon, node_lat, data.keys()) ]

    # Weights - Stars on graph
    fig_weights = [ plotWeight( line_id= key, 
                                a=line[0], b=line[1], w=line[2]) 
                                for key, line in zip(lines.keys(), lines.values() )]
    
    # Arrows - Direction of paths
    fig_arrows = [ plotArrow(line_id=key, a=line[0], b=line[1]) 
                for key, line in zip(lines.keys(),lines.values()) ]

    # There are 2 updating layer, 1-RedNodes, 2-TotalWeight
    place_holder = [ go.Scatter() ]

    
    fig_data = []
    fig_data.append( fig_lines )
    fig_data.append( fig_nodes )
    fig_data.append( fig_weights )
    fig_data.append( place_holder ) #One for Red Nodes on top
    fig_data.append( place_holder ) #One for TotalWeightSign


    fig_data_flat = list(itertools.chain.from_iterable(fig_data))    

    fig_layout = drawArrow(fig_arrows)


    return dict(data=fig_data_flat, layout=fig_layout), [min(node_lon), max(node_lat)], fig_data, fig_arrows






############## S T A R T    C O D E ######################################################################################

hdfs = connect_to_hdfs()

consumer = conectKafkaConsumer()
producer = conectKafkaProducer()

# with open("data.json") as json_f:
#     data = json.load(json_f)

# json_file = "hdfs://namenode:8020/data_in"

data = readHDFS(hdfs, path='/data_in')

print "READ HDFS:\n", data




#MAP data to key:cords, to make smaller global data
# cords = { k: [v["lon"], v["lat"]] for k, v in zip(data.keys(), data.values()) }

# fig-represents Plot, totalWeightPos -position of totalWeight in graph
fig, totalWeightPos, fig_data, fig_arrows = createBasePlot(data)       


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
    [   
        html.Div([
            dcc.Dropdown(
                id ='input-drop1',
                options = [ {'label': x, 'value': x} for x in data.keys()],
                placeholder="Select Start Node",
                multi=True
            )
        ],style={'width': '25%', 'display': 'inline-block','vertical-align': 'middle'}),
        html.Div([
            dcc.Dropdown(
                id ='input-drop2',
                options = [ {'label': x, 'value': x} for x in data.keys()],
                placeholder="Select End Node",
                multi=True
            )            
        ],style={'width': '25%', 'display': 'inline-block','vertical-align': 'middle'}),
        html.Div([
            html.Button('Submit', id='button')
        ],style={'display': 'inline-block', 'vertical-align': 'middle'}),
        
        dcc.Graph(id='live-update-graph',
                  figure=fig,
                  animate=True
        ),        
        dcc.Interval(
            id='interval-component',
            interval=1000,
            n_intervals=0
        ),
        html.Div(id='output-keypress')        
        
    ]
)

def generator(consumer):
    
    #list of consumer messages+info
    batch = consumer.poll()

    # print batch
    
    new_edges = []
    path = []
    total_cost = -1

    for x in batch.values() :        
        for val in x:
            # ConsumerRecord = list(topic[0], partition[1], offset[2], timestamp[3], timestamp_type[4], 
            # key[5], value[6], headers[7], checksum[8], serialized_key_size[9], serialized_value_size]10,
            # serialized_header_size[11])                        
            
            if val[0] == TOPIC_EDGES:                
                # print type(val[6]), " val = ", val[6].split(), "\n"
                new_edges.append( val[6].split() )
            
            elif val[0] == TOPIC_RESULT:                
                total_cost = val[6].split()[0]
                path = val[6].split()[1:]

    return total_cost, path, new_edges

def deleteRedTrace(fig_data):
    

    # print "PRE\n", fig_data
    del fig_data[-2:]
    # print "POSLE\n", fig_data

    for line in fig_data[0]:
        #iterate over lines                        
        line["line"]["color"] = middle_blue #108, 163, 252
    
    for stars in fig_data[2]:
        stars["marker"]["color"] = yellow #188, 229, 252

    return fig_data


def changeItem(fig_data, node_ids, weight, color):

    item_id = str(node_ids[0])+"-"+str(node_ids[1])

    for i, line in enumerate(fig_data[0]):
        if line["name"] == ('l'+item_id):
        # Change WEIGHT and put color
            fig_data[2][i]["text"] = ['<b>'+str(weight)+'</b>']
            fig_data[2][i]["marker"]["color"] = color

            if color == red:
                line["line"]["color"] = red

            return fig_data, True

    # Item doesn't exist    
    return fig_data, False


def appendItem(fig_data, fig_arrows, node_ids, a, b, w):
    a = toFloat(a)
    b = toFloat(b)
    item_id = str(node_ids[0])+"-"+str(node_ids[1])

    a = checkCoor(fig_data[1][0], node_ids[0], a)
    b = checkCoor(fig_data[1][0], node_ids[1], b)

    # Append Line and Weight
    fig_data[0].append( plotLine(line_id=item_id, a=a, b=b) )
    fig_data[2].append( plotWeight(line_id=item_id, a=a, b=b, w=w) )

    # Append Nodes
    fig_data[1] = appendNode(fig_nodes=fig_data[1], node_id=node_ids[0], coor=a)
    fig_data[1] = appendNode(fig_nodes=fig_data[1], node_id=node_ids[1], coor=b)

    # Append Arrows
    fig_arrows.append( plotArrow(line_id=item_id, a=a, b=b) )

    return fig_data, fig_arrows

def checkCoor(nodes, node_id, coor):
    
    try:
        i = nodes["text"].index('<b>'+node_id+'</b>')
        coor = [ nodes['x'][i], nodes['y'][i] ]
    except:
        pass
    return coor

def appendNode(fig_nodes, node_id, coor):
    # fig_nodes is list with 1 element    
    fig_nodes = fig_nodes[0]
    # coor = toFloat(coor)

    s_node_id = '<b>' + str(node_id) + '</b>' 

    if s_node_id not in fig_nodes["text"]:
    # put Nodes if don't exist    
        fig_nodes["text"] += (s_node_id, )
        fig_nodes["x"] += (float(coor[0]), )
        fig_nodes["y"] += (float(coor[1]), )

    return [fig_nodes]
         

def newEdges(fig_data, fig_arrows, edges):
    # new_edges: start_id weight end_id start_lon start_lat end_lon end_lat
    #               0       1       2       3        4         5       6
    # print fig_data

    for edge in edges:
        print "EDGE: ", edge

        fig_data, succ = changeItem(fig_data=fig_data, node_ids=[ edge[0], edge[2] ], weight=edge[1], color=yellow)
        
        if succ == False:
            fig_data, fig_arrows = appendItem(fig_data=fig_data, fig_arrows=fig_arrows,
                                              node_ids=[ edge[0], edge[2] ],
                                              a=edge[3:5], b=edge[5:7], w=edge[1] )

    return fig_data, fig_arrows

def newPath(fig_data, path):
    # print total_cost,
    # path: start_id weight node_id ... weight end_id 
            
    node_ids = path[::2]     
    path_weights = [int(w) for w in path[1::2]]

    for i, weight in enumerate(path_weights):             

        fig_data, succ = changeItem(fig_data=fig_data, node_ids=[ node_ids[i], node_ids[i+1] ], weight=weight, color=red)
            
        if succ == False:
            print "Some part of PATH OF THE MAP !!! \n"

    return fig_data


def createRedNodes(fig_data, node_ids):
    
    red_lon = []
    red_lat = []
    for x in node_ids:
        i = fig_data[1][0]["text"].index('<b>'+x+'</b>')
        red_lon.append( fig_data[1][0]["x"][i] )
        red_lat.append( fig_data[1][0]["y"][i] )

    # global cords
    # red_lon = [ cords[node_id][0] for node_id in node_ids ] 
    # red_lat = [ cords[node_id][1] for node_id in node_ids ] 

    return go.Scatter(  x=red_lon,
                        y=red_lat,
                        mode='markers+text',
                        name="RedNodes",
                        marker=dict(
                            color= red,
                            symbol='circle',
                            size=35
                        ),
                        text= ['<b>'+x+'</b>' for x in node_ids ],
                        textposition='middle center',
                        textfont=dict(
                            # family='sans serif',
                            size=18,
                            color=white
                        )                          
            )

def putTotalWeight(fig_data, total_cost):
    # global totalWeightPos

    min_lon = min( fig_data[1][0]["x"] )
    max_lat = max( fig_data[1][0]["y"] )


    return go.Scatter(  x= [ min_lon ],
                        y= [ max_lat ],
                        mode='markers+text',
                        marker=dict(
                            color= red,
                            symbol='octagon',
                            size=70
                        ),
                        text= ['<b>'+total_cost+'</b>'],
                        textposition='middle center',
                        textfont=dict(
                            # family='sans serif',
                            size=18,
                            color=white
                        )                          
            )

# Update Dropdown list
@app.callback(
      [Output('input-drop1', 'options'),
       Output('input-drop2', 'options')],
     [Input('interval-component', 'n_intervals')]
)
def update_date_dropdown(n_intervals):
    global fig_data

    node_ids = fig_data[1][0]["text"]    
    opt = [{'label': n_id[3:-4], 'value': n_id[3:-4]} for n_id in node_ids]
    return opt, opt


@app.callback( Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])

def update_graph(n_intervals):

    global fig_data 
    global fig_arrows 

    total_cost, path, new_edges = generator(consumer)    

    if len(new_edges) > 0:  
        fig_data, fig_arrows = newEdges(fig_data=fig_data, fig_arrows=fig_arrows, edges=new_edges)
    
    if total_cost != -1:

        fig_data = deleteRedTrace(fig_data)
        
        fig_data = newPath(fig_data=fig_data, path=path)

        fig_data.append( [ createRedNodes(fig_data=fig_data, node_ids=path[::2]) ] )

        fig_data.append( [ putTotalWeight(fig_data=fig_data, total_cost=total_cost) ] )
    


    fig_data_flat = list(itertools.chain.from_iterable(fig_data))    
    fig["data"] = fig_data_flat
    fig["layout"] = drawArrow(fig_arrows)

    return fig
    

# Get Start ID -> End ID and send that to Spark to find shortest path
@app.callback( Output('output-keypress', 'children'),
               [Input('button', 'n_clicks'),
                Input('input-drop1', 'value'),
                Input('input-drop2', 'value')])
def update_output(n_click, input1, input2):

    global n_click_previous

    if n_click != n_click_previous:
        n_click_previous = n_click
        if input1 != None and input2 != None:

            input1 = ' '.join(input1)
            input2 = ' '.join(input2)

            msg = str(input1)+"\n"+str(input2)
            producer.send(TOPIC_START, value=msg, key=msg)
            print "START NODES: ", input1
            print "END NODES: ", input2
    
    return u'Start Node is "{}" and End Node is "{}"'.format(input1, input2)



if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port='99') #85 ,