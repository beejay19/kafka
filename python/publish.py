import commands
import re
import json


def returnDict(row):
    """
    """

    contextId,originatingApplicationId,triggerId,phoneNo,yy,mm,dd = row.split(",")

    dic = dict( contextId = contextId, originatingApplicationId = originatingApplicationId, triggerId = triggerId, eventId = dict (phoneNo = phoneNo)   )   

    return dic



query = "Select * from ebb.transactions"

host=str('ec2-34-201-210-90.compute-1.amazonaws.com')
port=str('10000')
authMechanism=str('KERBEROS')
database=str('ebb')
principal=str('hive/ec2-34-201-210-90.compute-1.amazonaws.com@HDP.COM')

result_string= 'beeline -u "jdbc:hive2://"'+host +'":"'+port+'"/"'+database+'";principal="'+principal+'"" ' \
'--fastConnect=true --showHeader=false --outputformat=csv2 --verbose=false --showWarnings=false --silent=true ' \
'-e "' + query + ';"'

status, output = commands.getstatusoutput(result_string)

if status == 0:
    with open("out.csv", "w") as f:
        f.write(re.sub(".*Java.HotSpot.*?\n","",output))
        f.close()
else:
    print "Error encountered while executing HiveQL queries."


with open('out.csv',"r") as f:
    with open('publish.csv',"w") as p:
        for line in f:
            final_msg = returnDict(line)
            print(final_msg)
            p.write(json.dumps(final_msg) + "\n")


publishcmd="kafka-console-producer --broker-list ec2-34-201-210-90.compute-1.amazonaws.com:9092 --topic streams < publish.csv"

status, output = commands.getstatusoutput(publishcmd)

