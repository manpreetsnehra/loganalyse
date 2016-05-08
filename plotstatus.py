#!/usr/bin/python

import matplotlib.pyplot as plt
import matplotlib.dates as ds
import numpy as np
import sqlite3
import sys

try: 
    conn = sqlite3.connect('accesslogs.sqlite')
except:
    print "Logs database file not found"
    sys.exit(1)
    
cur=conn.cursor()

cur.execute('''SELECT date,data from Dailystatus order by date asc''')
alldata=cur.fetchall()
firstdate=alldata[0][0]
data=[]
date=[]
for status in alldata:
    data.append(status[1])
    date.append(status[0]-firstdate)

npdate=np.array(date)
npdata=np.array(data)
    
#plt.plot(date,data)
#plt.xlabel('Date')
#plt.ylabel('Data Sent')
#plt.title('Data Sent Per Day')
#plt.grid(False)
#plt.savefig("status.png")
#plt.show()

