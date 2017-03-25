#!/usr/bin/python
#process the status codes in the logs table based on date. How many of which status codes per day
import sqlite3
import string
import sys

status_dict={200:"Code200",206:"Code206",301:"Code301",302:"Code302",304:"Code304",400:"Code400",403:"Code403",404:"Code404",405:"Code405",413: "Code413",500:"Code500",501:"Code501"}

try: 
    conn = sqlite3.connect('accesslogs.sqlite')
except:
    print "Logs database file not found"
    sys.exit(1)
cur=conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS DailyStatus 
            (date INTEGER UNIQUE PRIMARY KEY, Code200 INTEGER DEFAULT 0, Code206 INTEGER DEFAULT 0, Code301 INTEGER DEFAULT 0, Code302 INTEGER DEFAULT 0,
            Code304 INTEGER DEFAULT 0, Code400 INTEGER DEFAULT 0, Code403 INTEGER DEFAULT 0, Code404 INTEGER DEFAULT 0, Code405 INTEGER DEFAULT 0, 
            Code413 INTEGER DEFAULT 0, Code500 INTEGER DEFAULT 0, Code501 INTEGER DEFAULT 0, data REAL DEFAULT 0)''')

cur.execute('''select date from Logs''')
alldates=cur.fetchall()

for date in alldates:
    cur.execute('''INSERT OR IGNORE INTO DailyStatus (date) VALUES (?)''',(date[0],))
conn.commit()
while True:
	cur.execute('''SELECT Logs.id,Logs.date,Logs.status,Logs.bytes_sent from Logs JOIN LogsProcessed ON Logs.id=LogsProcessed.id where Logsprocessed.status=0 LIMIT 10000''')
	allrecords=cur.fetchall()
	if allrecords == []:
		print "All records processed"
		break
	for record in allrecords:
		print record[0]
		seperator=" "
		query1=seperator.join(["SELECT",status_dict[record[2]],"from Dailystatus where date =",str(record[1])])
		cur.execute(query1)
		codestatus=cur.fetchone()
		statuscount = codestatus[0]+1
		query2=seperator.join(["Update Dailystatus set",str(status_dict[record[2]]),"=",str(statuscount),"where date =",str(record[1])])
		cur.execute(query2)
		cur.execute('''SELECT data from Dailystatus where date = ?''',(record[1],))
		total_data=cur.fetchone()[0]/(1024*1024)
		data=float(total_data)+float(record[3])
		cur.execute('''UPDATE Dailystatus set data =  ? where date = ?''',(data,record[1],))
		cur.execute('''UPDATE LogsProcessed set status=1 where id = ?''',(record[0],))
	conn.commit()
conn.commit()
