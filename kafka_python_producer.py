from time import sleep
from json import dumps
from kafka import KafkaProducer

def createDict(data):
    fields = data.rstrip("\n").split(",")
    return { 'custID': fields[0], 'itemID': fields[1], 'amount': fields[2]}

# Creating Producer for this program
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

#File Location
fileloc="C:/Users/yyyyy/Desktop/git_projects/RealTimeDataAnalysisApp1/customer-orders.csv"


with open(fileloc) as file: 
    for raw in file:
        data = createDict(raw)
        print(data)
        producer.send('CustOrder', value=data)
        sleep(5)

