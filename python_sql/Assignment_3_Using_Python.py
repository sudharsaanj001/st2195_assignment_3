import sqlite3
import os
# Used to set the working directory. Include an additional "\" between folders for the code to work. 
# os.chdir("C:\Users\Sudharsaan\OneDrive\SIM\Year 2\ST2195 Programming for Data Science\Practice Assignment\Assignment 3\Harverd Dataverse") does not work
os.chdir("C:\\Users\\Sudharsaan\\OneDrive\\SIM\\Year 2\\ST2195 Programming for Data Science\\Practice Assignment\\Assignment 3\\Harverd Dataverse")
os.getcwd() # Check current working directory
ls # Can also be used to check current working directory & give additional details. 
# Creating airline3.db database & establishing connection
conn = sqlite3.connect("airline3.db")

import pandas as pd

ontime00 = pd.read_csv("2000.csv.bz2")
ontime01 = pd.read_csv("2001.csv.bz2", encoding="latin-1") # I had a unicodedecodeerror so I had to use this random code I found online
ontime02 = pd.read_csv("2002.csv.bz2", encoding="latin-1")
ontime03 = pd.read_csv("2003.csv.bz2", encoding="latin-1")
ontime04 = pd.read_csv("2004.csv.bz2", encoding="latin-1")
ontime05 = pd.read_csv("2005.csv.bz2", encoding="latin-1")

ontime01.to_sql("ontime", con = conn, index = False )
ontime02.to_sql("ontime", conn, index = False, if_exists = "append")
ontime03.to_sql("ontime", conn, index = False, if_exists = "append")
ontime00.to_sql("ontime", conn, index = False, if_exists = "append")
ontime04.to_sql("ontime", conn, index = False, if_exists = "append")
ontime05.to_sql("ontime", conn, index = False, if_exists = "append")

planesdata_db = pd.read_csv("plane-data.csv")
planesdata_db.to_sql("planes", conn, index = False)

carriers_db = pd.read_csv("carriers.csv")
carriers_db.to_sql("carriers", conn, index = False)

airports_db = pd.read_csv("airports.csv")
airports_db.to_sql("airports", conn, index = False)

c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()

q1 = c.execute(''' 
               SELECT model, AVG(DepDelay) as Avg_DepDelay
               FROM ontime JOIN planes on ontime.TailNum = planes.tailnum
               WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
               GROUP BY model
               ORDER BY Avg_DepDelay
               LIMIT 3''').fetchall()
pd.DataFrame(q1)
# Output          
#      0          1
# 0  737-2Y5   7.022026
# 1  737-282   8.433566
# 2  737-230  10.458647
# Thus, plane model with lowest associated departure delay is 737-2Y5 with departure delayof 7.022026

q2 = c.execute('''
               SELECT city, ontime.Dest as Destination, COUNT(*) as inbound_flights
               FROM ontime JOIN airports on ontime.Dest = airports.iata
               WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0
               GROUP BY ontime.Dest
               ORDER BY inbound_flights DESC
               LIMIT 3''').fetchall()
pd.DataFrame(q2)            
# Output 
#                    0    1        2
# 0            Chicago  ORD  1963034
# 1            Atlanta  ATL  1909041
# 2  Dallas-Fort Worth  DFW  1776144
# Thus, city with highest number of inbound flights is Chicago with 1963034 inbound flights

q3 = c.execute('''
               SELECT ontime.UniqueCarrier as Carrier, Description, COUNT(*) as Cancelled_Flights
               FROM ontime JOIN carriers on ontime.UniqueCarrier = carriers.code
               WHERE ontime.Cancelled = 1
               GROUP BY ontime.UniqueCarrier
               ORDER BY Cancelled_Flights DESC
               LIMIT 3''').fetchall()
pd.DataFrame(q3)
# Output
#     0                       1       2
# 0  DL    Delta Air Lines Inc.  107851
# 1  AA  American Airlines Inc.  105762
# 2  UA   United Air Lines Inc.  102066
# Thus, carrier with highest number of cancelled flights is Delta Air Lines with 107851 cancelled flights. 































