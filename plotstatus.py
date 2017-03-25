#!/usr/bin/python

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
import sys

try: 
	conn = sqlite3.connect('accesslogs.sqlite')
except:
	print "Logs database file not found"
	sys.exit(1)

cur=conn.cursor()

cur.execute('''SELECT * from Dailystatus order by date asc''')
header=[]
for columns in cur.description:
	header.append(columns[0])
alldata=cur.fetchall()
firstdate=alldata[0][0]
#plot All the codes and data sent as long as number of responses of a type are more than 5
type=1
seperator=""
while type < 14:
	sum=0
	data=[]
	date=[]
	for status in alldata:
		data.append(status[type])
		datenew=pd.to_datetime(str(status[0]), format='%Y%m%d')
		date.append(datenew)
		sum=sum+status[type]
	if sum > 5:
		npdate=np.array(date)
		npdata=np.array(data)
		plt.plot(date,data)
		plt.xlabel('Date')
		plt.ylabel(header[type])
		plt.title(seperator.join([header[type],' Per Day']))
		plt.grid(True)
		filename=seperator.join([str(header[type]),".png"])
		# beautify the x-labels
		plt.gcf().autofmt_xdate()
		plt.savefig(filename)
		plt.show()
	type=type+1
		
