from time import sleep

def createDict(data):
    fields = data.rstrip("\n").split(",")
    return { 'custID': fields[0], 'itemID': fields[1], 'amount': fields[2]}

fileloc='C:/Users/yyyyy/Desktop/git_projects/RealTimeDataAnalysisApp1/customer-orders.csv'

with open(fileloc) as file: 
    for i in file:
        print(createDict(i))
        sleep(5)