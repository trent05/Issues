import requests
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import pandas as pd
r = requests.get('http://www.citibikenyc.com/stations/json')
key_list = []
for station in r.json()['stationBeanList']:
 for k in station.keys():
  if k not in key_list:
   key_list.append(k)
#print key_list
df = json_normalize(r.json()['stationBeanList'])
#df['statusKey'].hist()
#plt.show()
#print df[df['statusValue'] == 'In Service']['totalDocks'].mean()
#1. Create a table to store data
import sqlite3 as lite
con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
 cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
#2. Populate table with values
sql = "INSERT INTO  citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
with con:
 for station in r.json()['stationBeanList']:
  cur.execute(sql, (station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))
#3. Convert ID to a column (and add something other than a number to the beginning)
station_ids = df['id'].tolist()
station_ids = ['_' + str(x) + ' INT' for x in station_ids]
with con:
 cur.execute("CREATE TABLE available_bikes ( execution_time INT, " + ", ".join(station_ids) + ");")
#4. Populate values for available bikes
import time
from dateutil.parser import parse
import collections
exec_time = parse(r.json()['executionTime'])
#5. Insert execution time into the database
with con:
 cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
#6. Iterate through stations in the "stationBeanList"
id_bikes = collections.defaultdict(int)
for station in r.json()['stationBeanList']:
 id_bikes[station['id']] = station['availableBikes']
with con:
 for k, v in id_bikes.iteritems():
  cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%Y-%m-%dT%H:%M:%S') + "T" + exec_time.strftime('%Y-%m-%d') + ";")